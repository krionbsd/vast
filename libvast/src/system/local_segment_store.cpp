//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/local_segment_store.hpp"

#include "vast/detail/overload.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/query.hpp"
#include "vast/segment_store.hpp"
#include "vast/span.hpp"
#include "vast/system/node_control.hpp"
#include "vast/table_slice.hpp"

#include <caf/settings.hpp>
#include <flatbuffers/flatbuffers.h>
#include <fmt/format.h>

#include <vector>

namespace vast::system {

namespace {

// Handler for `vast::query` that is shared between active and passive stores.
template <typename Actor>
caf::result<atom::done>
handle_lookup(Actor& self, const vast::query& query, const vast::ids& ids,
              const std::vector<table_slice>& slices) {
  std::vector<expression> checkers;
  for (const auto& slice : slices) {
    if (query.expr == expression{}) {
      checkers.emplace_back();
    } else {
      auto c = tailor(query.expr, slice.layout());
      if (!c)
        return c.error();
      checkers.emplace_back(prune_meta_predicates(std::move(*c)));
    }
  }
  auto zipped = detail::zip(slices, checkers);
  caf::visit(detail::overload{
               [&](const query::count& count) {
                 if (count.mode == query::count::estimate)
                   die("logic error detected");
                 for (size_t i = 0; i < slices.size(); ++i) {
                   const auto& slice = slices.at(i);
                   const auto& checker = checkers.at(i);
                   auto result = count_matching(slice, checker, ids);
                   self->send(count.sink, result);
                 }
               },
               [&](const query::extract& extract) {
                 for (const auto& [slice, checker] : zipped) {
                   if (extract.policy == query::extract::preserve_ids) {
                     for (auto& sub_slice : select(slice, ids)) {
                       if (query.expr == expression{}) {
                         self->send(extract.sink, sub_slice);
                       } else {
                         auto hits = evaluate(checker, sub_slice);
                         for (auto& final_slice : select(sub_slice, hits))
                           self->send(extract.sink, final_slice);
                       }
                     }
                   } else {
                     // TODO: Make something like foreach_
                     auto final_slice = filter(slice, checker, ids);
                     if (final_slice)
                       self->send(extract.sink, *final_slice);
                   }
                 }
               },
               [&](query::erase) {
                 // The caller should have special-cased this before calling.
                 VAST_ASSERT(false, "cant lookup an 'erase' query");
               },
             },
             query.cmd);
  return atom::done_v;
}

} // namespace

std::filesystem::path store_path_for_partition(const uuid& partition_id) {
  auto store_filename = fmt::format("{}.store", partition_id);
  return std::filesystem::path{"archive"} / store_filename;
}

store_actor::behavior_type
passive_local_store(store_actor::stateful_pointer<passive_store_state> self,
                    filesystem_actor fs, const std::filesystem::path& path) {
  // TODO: We probably want 'read' rather than 'mmap' here for
  // predictable performance.
  self->set_exit_handler([self](const caf::exit_msg&) {
    for (auto&& [expr, ids, rp] :
         std::exchange(self->state.deferred_requests, {}))
      rp.deliver(caf::make_error(ec::lookup_error, "partition store shutting "
                                                   "down"));
  });
  self->request(fs, caf::infinite, atom::mmap_v, path)
    .then([self](chunk_ptr chunk) {
      // self->state.data = std::move(chunk);
      auto seg = segment::make(std::move(chunk));
      if (!seg) {
        VAST_ERROR("couldnt create segment from chunk: {}", seg.error());
        self->send_exit(self, caf::exit_reason::unhandled_exception);
        return;
      }
      self->state.segment = std::move(*seg);
      // Delegate all deferred evaluations now that we have the partition chunk.
      VAST_DEBUG("{} delegates {} deferred evaluations", self,
                 self->state.deferred_requests.size());
      for (auto&& [expr, ids, rp] :
           std::exchange(self->state.deferred_requests, {}))
        rp.delegate(static_cast<store_actor>(self), std::move(expr),
                    std::move(ids));
    });
  return {
    // store
    [self](query query, ids ids) -> caf::result<atom::done> {
      VAST_WARN("got a query for some ids");
      if (!self->state.segment) {
        auto rp = caf::typed_response_promise<atom::done>();
        self->state.deferred_requests.emplace_back(query, ids, rp);
        return rp;
      }
      // Special-case handling for "erase"-queries because their
      // implementation must be different depending on if we operate
      // in memory or on disk.
      if (caf::holds_alternative<query::erase>(query.cmd)) {
        return self->delegate(static_cast<store_actor>(self), atom::erase_v,
                              ids);
      }
      auto slices = self->state.segment->lookup(ids);
      if (!slices)
        return slices.error();
      return handle_lookup(self, query, ids, *slices);
    },
    [self](atom::erase, ids xs) -> caf::result<atom::done> {
      if (!self->state.segment) {
        // Treat this as an "erase" query for the purposes of storing it
        // until the segment is loaded.
        auto rp = caf::typed_response_promise<atom::done>();
        self->state.deferred_requests.emplace_back(query::make_erase({}), xs,
                                                   rp);
        return rp;
      }
      auto new_segment = segment::copy_without(*self->state.segment, xs);
      if (!new_segment) {
        VAST_ERROR("could not remove ids from segment {}: {}",
                   self->state.segment->id(), render(new_segment.error()));
        return new_segment.error();
      }
      VAST_ASSERT(self->state.path.has_filename());
      auto old_path = self->state.path;
      auto new_path = self->state.path.replace_extension("next");
      // TODO: If the new segment is empty, we should probably just erase the
      // file without replacement here.
      self
        ->request(self->state.fs, caf::infinite, atom::write_v, new_path,
                  new_segment->chunk())
        .then(
          [seg = std::move(*new_segment), self, old_path,
           new_path](atom::ok) mutable {
            std::error_code ec;
            // Re-use the old filename so that we don't have to write a new
            // partition flatbuffer with the changed store header as well.
            std::filesystem::rename(new_path, old_path, ec);
            if (ec)
              VAST_ERROR("failed to erase old data {}", seg.id());
            self->state.segment = std::move(seg);
          },
          [](caf::error& err) {
            VAST_ERROR("failed to flush archive {}", to_string(err));
          });
      return atom::done_v;
    },
  };
}

store_builder_actor::behavior_type active_local_store(
  store_builder_actor::stateful_pointer<active_store_state> self,
  filesystem_actor fs, const std::filesystem::path& path) {
  VAST_INFO("spawning active local store active"); // FIXME: INFO -> DEBUG
  // TODO: The shutdown path is copied from the archive; align it
  // with the fs actor.
  self->state.builder
    = std::make_unique<segment_builder>(defaults::system::max_segment_size);
  self->set_exit_handler([self, path, fs](const caf::exit_msg&) {
    VAST_INFO("exiting active store");
    auto seg = self->state.builder->finish();
    self->request(fs, caf::infinite, atom::write_v, path, seg.chunk())
      .then([](atom::ok) { /* nop */ },
            [self](caf::error& err) {
              VAST_ERROR("failed to flush archive {}", to_string(err));
            });
    self->quit();
  });

  return {
    // store
    [self](vast::query query, const ids& ids) -> caf::result<atom::done> {
      auto slices = self->state.builder->lookup(ids);
      if (!slices)
        return slices.error();
      if (caf::holds_alternative<query::erase>(query.cmd)) {
        return self->delegate(static_cast<store_actor>(self), atom::erase_v,
                              ids);
      }
      return handle_lookup(self, query, ids, *slices);
    },
    [self](atom::erase, const ids& ids) -> caf::result<atom::done> {
      auto seg = self->state.builder->finish();
      auto id = seg.id();
      auto slices = seg.erase(ids);
      if (!slices)
        return slices.error();
      self->state.builder->reset(id);
      for (auto&& slice : std::exchange(*slices, {}))
        self->state.builder->add(std::move(slice));
      return atom::done_v;
    },
    // store builder
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      return self
        ->make_sink(
          in, [=](caf::unit_t&) {},
          [=](caf::unit_t&, std::vector<table_slice>& batch) {
            VAST_TRACE("{} gets batch of {} table slices", self, batch.size());
            for (auto& slice : batch)
              if (auto error = self->state.builder->add(slice))
                VAST_ERROR("{} failed to add table slice to store {}", self,
                           render(error));
          },
          [=](caf::unit_t&, const caf::error&) {

          })
        .inbound_slot();
    },
    // Conform to the protocol of the STATUS CLIENT actor.
    [self](atom::status,
           status_verbosity) -> caf::dictionary<caf::config_value> {
      return {};
    },
  };
}

class local_store_plugin final : public virtual store_plugin {
public:
  using store_plugin::builder_and_header;

  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "local_segment_store";
  };

  // store plugin API
  caf::error setup(const node_actor& node) override {
    caf::scoped_actor self{node.home_system()};
    auto maybe_components = get_node_components<filesystem_actor>(self, node);
    if (!maybe_components)
      return maybe_components.error();
    fs_ = std::get<0>(*maybe_components);
    return caf::none;
  }

  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(const vast::uuid& id) const override {
    auto path = store_path_for_partition(id);
    std::string path_str = path.string();
    auto header = chunk::make(std::move(path_str));
    // TODO: Would it make sense to pass an actor_system& as first arg?
    auto builder = fs_->home_system().spawn(active_local_store, fs_, path);
    return builder_and_header{std::move(builder), std::move(header)};
  }

  [[nodiscard]] virtual caf::expected<system::store_actor>
  make_store(span<const std::byte> header) const override {
    std::string_view sv{reinterpret_cast<const char*>(header.data()),
                        header.size()};
    std::filesystem::path path{sv};
    // TODO: Would it make sense to pass an actor_system& as first arg?
    return fs_->home_system().spawn(passive_local_store, fs_, path);
  }

private:
  filesystem_actor fs_;
};

VAST_REGISTER_PLUGIN(vast::system::local_store_plugin)

} // namespace vast::system