//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/lru_cache.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/fbs/index.hpp"
#include "vast/plugin.hpp"
#include "vast/query.hpp"
#include "vast/system/active_partition.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/meta_index.hpp"
#include "vast/system/query_cursor.hpp"
#include "vast/uuid.hpp"

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/meta/omittable_if_empty.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <queue>
#include <unordered_map>
#include <vector>

namespace vast::system {

/// Extract a partition synopsis from the partition at `partition_path`
/// and write it to `partition_synopsis_path`.
//  TODO: Move into separate header.
caf::error
extract_partition_synopsis(const std::filesystem::path& partition_path,
                           const std::filesystem::path& partition_synopsis_path);

/// Flatbuffer integration. Note that this is only one-way, restoring
/// the index state needs additional runtime information.
// TODO: Pull out the persisted part of the state into a separate struct
// that can be packed and unpacked.
caf::expected<flatbuffers::Offset<fbs::Index>>
pack(flatbuffers::FlatBufferBuilder& builder, const index_state& state);

/// The state of the active partition.
struct active_partition_info {
  /// The partition actor.
  active_partition_actor actor = {};

  /// The slot ID that identifies the partition in the stream.
  caf::stream_slot stream_slot = {};

  /// The store actor that holds the segments for this partition.
  // NOTE: Logically this should belong inside the active partition, but the way
  // the CAF streaming api works makes it really annoying to have the partition
  // stream both whole table_slices to the store and table_slice_columns to
  // the indexers. So barring a major refactoring, we just have the partition
  // do the streaming.
  store_builder_actor store = {};

  // The slot ID that identifies the store in the stream.
  caf::stream_slot store_slot = {};

  /// The remaining free capacity of the partition.
  size_t capacity = {};

  /// The UUID of the partition.
  uuid id = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, active_partition_info& x) {
    return f(caf::meta::type_name("active_partition_info"), x.actor,
             x.stream_slot, x.capacity, x.id);
  }
};

/// Accumulates statistics for a given layout.
struct layout_statistics {
  uint64_t count; ///< Number of events indexed.

  template <class Inspector>
  friend auto inspect(Inspector& f, layout_statistics& x) {
    return f(caf::meta::type_name("layout_statistics"), x.count);
  }
};

/// Accumulates statistics about indexed data.
struct index_statistics {
  /// The number of events for a given layout.
  std::unordered_map<std::string, layout_statistics> layouts;

  template <class Inspector>
  friend auto inspect(Inspector& f, index_statistics& x) {
    return f(caf::meta::type_name("index_statistics"), x.layouts);
  }
};

/// Loads partitions from disk by UUID.
class partition_factory {
public:
  explicit partition_factory(index_state& state);

  filesystem_actor& filesystem(); // getter/setter

  partition_actor operator()(const uuid& id) const;

private:
  filesystem_actor filesystem_;
  const index_state& state_;
};

struct query_backlog {
  struct job {
    vast::query query;
    caf::typed_response_promise<query_cursor> rp;
  };

  // Emplace a job.
  void emplace(vast::query query, caf::typed_response_promise<query_cursor> rp);

  [[nodiscard]] std::optional<job> take_next();

  std::queue<job> normal;
  std::queue<job> low;
};

struct query_state {
  /// The UUID of the query.
  vast::uuid id;

  /// The query expression.
  vast::query query;

  /// Unscheduled partitions.
  std::vector<uuid> partitions;

  /// The assigned query worker.
  query_supervisor_actor worker;

  template <class Inspector>
  friend auto inspect(Inspector& f, query_state& x) {
    return f(caf::meta::type_name("query_state"), x.id, x.query,
             caf::meta::omittable_if_empty(), x.partitions, x.worker);
  }
};

/// The state of the index actor.
struct index_state {
  // -- type aliases -----------------------------------------------------------

  using index_stream_stage_ptr
    = caf::stream_stage_ptr<table_slice,
                            caf::broadcast_downstream_manager<table_slice>>;

  // -- constructor ------------------------------------------------------------

  explicit index_state(index_actor::pointer self);

  // -- persistence ------------------------------------------------------------

  [[nodiscard]] std::filesystem::path
  index_filename(const std::filesystem::path& basename = {}) const;

  // Maps partitions to their expected location on the file system.
  [[nodiscard]] std::filesystem::path partition_path(const uuid& id) const;

  // Maps partition synopses to their expected location on the file system.
  [[nodiscard]] std::filesystem::path
  partition_synopsis_path(const uuid& id) const;

  caf::error load_from_disk();

  void flush_to_disk();

  // -- query handling ---------------------------------------------------------

  [[nodiscard]] bool worker_available() const;

  [[nodiscard]] std::optional<query_supervisor_actor> next_worker();

  /// Get the actor handles for up to `num_partitions` PARTITION actors,
  /// spawning them if needed.
  [[nodiscard]] std::vector<std::pair<uuid, partition_actor>>
  collect_query_actors(query_state& lookup, uint32_t num_partitions);

  // -- flush handling ---------------------------------------------------------

  /// Adds a new flush listener.
  void add_flush_listener(flush_listener_actor listener);

  /// Sends a notification to all listeners and clears the listeners list.
  void notify_flush_listeners();

  // -- partition handling -----------------------------------------------------

  /// Generates a unique query id.
  vast::uuid create_query_id();

  /// Creates a new active partition.
  void create_active_partition();

  /// Decommissions the active partition.
  void decomission_active_partition();

  // -- introspection ----------------------------------------------------------

  /// Flushes collected metrics to the accountant.
  void send_report();

  /// @returns various status metrics.
  [[nodiscard]] caf::typed_response_promise<record>
  status(status_verbosity v) const;

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  index_actor::pointer self;

  /// The streaming stage.
  index_stream_stage_ptr stage;

  /// The single active (read/write) partition.
  active_partition_info active_partition = {};

  /// Partitions that are currently in the process of persisting.
  // TODO: An alternative to keeping an explicit set of unpersisted partitions
  // would be to add functionality to the LRU cache to "pin" certain items.
  // Then (assuming the query interface for both types of partition stays
  // identical) we could just use the same cache for unpersisted partitions and
  // unpin them after they're safely on disk.
  std::unordered_map<uuid, partition_actor> unpersisted = {};

  /// The set of passive (read-only) partitions currently loaded into memory.
  /// Uses the `partition_factory` to load new partitions as needed, and evicts
  /// old entries when the size exceeds `max_inmem_partitions`.
  detail::lru_cache<uuid, partition_actor, partition_factory> inmem_partitions;

  /// The set of partitions that exist on disk.
  std::unordered_set<uuid> persisted_partitions = {};

  /// This set to true after the index finished reading the meta index state
  /// from disk.
  bool accept_queries = {};

  /// Whether we should use a partition-local store for the active partition.
  bool partition_local_stores = {};

  /// The maximum number of events that a partition can hold.
  size_t partition_capacity = {};

  /// The maximum size of the partition LRU cache (or the maximum number of
  /// read-only partition loaded to memory).
  size_t max_inmem_partitions = {};

  /// The number of partitions initially returned for a query.
  size_t taste_partitions = {};

  /// The set of received but unprocessed queries.
  query_backlog backlog = {};

  /// Maps query IDs to pending lookup state.
  std::unordered_map<uuid, query_state> pending = {};

  /// Maps exporter actor address to known query ID for monitoring
  /// purposes.
  std::unordered_map<caf::actor_addr, std::unordered_set<uuid>> monitored_queries
    = {};

  /// The number of query supervisors.
  size_t workers = 0;

  /// Caches idle workers.
  detail::stable_set<query_supervisor_actor> idle_workers = {};

  /// The META INDEX actor.
  meta_index_actor meta_index = {};

  /// A running count of the size of the meta index.
  size_t meta_index_bytes = {};

  /// The directory for persistent state.
  std::filesystem::path dir = {};

  /// The directory for partition synopses.
  std::filesystem::path synopsisdir = {};

  /// Statistics about processed data.
  index_statistics stats = {};

  /// Handle of the accountant.
  accountant_actor accountant = {};

  /// List of actors that wait for the next flush event.
  std::vector<flush_listener_actor> flush_listeners = {};

  /// Actor handle of the store actor.
  archive_actor global_store = {};

  /// Actor handle of the importer actor to reserve additional
  /// parts of the id space.
  idspace_distributor_actor importer = {};

  /// Plugin responsible for spawning new partition-local stores.
  const vast::store_plugin* store_plugin = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor filesystem = {};

  /// Config options to be used for new synopses; passed to active partitions.
  caf::settings synopsis_opts;

  /// Config options for the index.
  caf::settings index_opts;

  constexpr static inline auto name = "index";
};

/// Indexes events in horizontal partitions.
/// @param accountant The accountant actor.
/// @param filesystem The filesystem actor. Not used by the index itself but
/// forwarded to partitions.
/// @param archive The legacy archive actor. To be removed eventually (tm).
/// @param meta_index The meta index actor.
/// @param dir The directory of the index.
/// @param store_backend The store backend to use for new partitions.
/// @param partition_capacity The maximum number of events per partition.
/// @param max_inmem_partitions The maximum number of passive partitions loaded
/// into memory.
/// @param taste_partitions How many lookup partitions to schedule immediately.
/// @param num_workers The maximum amount of concurrent lookups.
/// @param meta_index_dir The directory used by the meta index.
/// @param synopsis_fp_rate The false positive rate for new address and string
/// synopses.
/// @pre `partition_capacity > 0
//  TODO: Use a settings struct for the various parameters.
index_actor::behavior_type
index(index_actor::stateful_pointer<index_state> self,
      accountant_actor accountant, filesystem_actor filesystem,
      archive_actor archive, meta_index_actor meta_index,
      const std::filesystem::path& dir, std::string store_backend,
      size_t partition_capacity, size_t max_inmem_partitions,
      size_t taste_partitions, size_t num_workers,
      const std::filesystem::path& meta_index_dir, double synopsis_fp_rate);

} // namespace vast::system

namespace fmt {

template <>
struct formatter<vast::system::query_state> : formatter<std::string> {
  template <class FormatContext>
  auto format(const vast::system::query_state& value, FormatContext& ctx) {
    return formatter<std::string>::format(caf::deep_to_string(value), ctx);
  }
};

} // namespace fmt
