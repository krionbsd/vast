/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstdint>
#include <chrono>
#include <vector>

#include "vast/logger.hpp"

#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/event.hpp"
#include "vast/format/writer.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/query_status.hpp"

namespace vast::system {

// The base class for SINK actors.
template <class Writer>
struct sink_state {
  std::chrono::steady_clock::duration flush_interval = std::chrono::seconds(1);
  std::chrono::steady_clock::time_point last_flush;
  uint64_t processed = 0;
  uint64_t max_events = 0;
  caf::event_based_actor* self;
  accountant_type accountant;
  vast::system::measurement measurement;
  Writer writer;
  const char* name = "writer";

  sink_state(caf::event_based_actor* self_ptr) : self(self_ptr) {
    // nop
  }

  void send_report() {
    if (accountant && measurement.events > 0) {
      auto r = performance_report{{{std::string{name}, measurement}}};
      measurement = {};
      self->send(accountant, std::move(r));
    }
  }
};

template <class Writer>
caf::behavior sink(caf::stateful_actor<sink_state<Writer>>* self,
                   Writer&& writer, uint64_t max_events) {
  static_assert(std::is_base_of_v<format::writer, Writer>);
  using namespace std::chrono;
  auto& st = self->state;
  st.writer = std::move(writer);
  st.name = st.writer.name();
  st.last_flush = steady_clock::now();
  if (max_events > 0) {
    VAST_DEBUG(self, "caps event export at", max_events, "events");
    st.max_events = max_events;
  } else {
    // Interpret 0 as infinite.
    st.max_events = std::numeric_limits<uint64_t>::max();
  }
  self->set_exit_handler(
    [=](const caf::exit_msg& msg) {
      self->state.send_report();
      self->quit(msg.reason);
    }
  );
  return {
    [=](std::vector<event>& xs) {
      VAST_DEBUG(self, "got:", xs.size(), "events from",
                 self->current_sender());
      auto& st = self->state;
      auto reached_max_events = [&] {
        VAST_INFO(self, "reached max_events:", st.max_events, "events");
        st.writer.flush();
        st.send_report();
        self->quit();
      };
      // Drop excess elements.
      auto remaining = st.max_events - st.processed;
      if (remaining == 0)
        return reached_max_events();
      if (xs.size() > remaining)
        xs.resize(remaining);
      // Handle events.
      auto t = timer::start(st.measurement);
      if (auto err = st.writer.write(xs)) {
        VAST_ERROR(self, self->system().render(err));
        self->quit(std::move(err));
        return;
      }
      t.stop(xs.size());
      // Stop when reaching configured limit.
      st.processed += xs.size();
      if (st.processed >= st.max_events)
        return reached_max_events();
      // Force flush if necessary.
      auto now = steady_clock::now();
      if (now - st.last_flush > st.flush_interval) {
        st.writer.flush();
        st.last_flush = now;
        st.send_report();
      }
    },
    [=](const uuid& id, const query_status&) {
      VAST_IGNORE_UNUSED(id);
      VAST_DEBUG(self, "got query statistics from", id);
    },
    [=](limit_atom, uint64_t max) {
      VAST_DEBUG(self, "caps event export at", max, "events");
      if (self->state.processed < max)
        self->state.max_events = max;
      else
        VAST_WARNING(self, "ignores new max_events of", max,
                     "(already processed", self->state.processed, " events)");
    },
    [=](accountant_type accountant) {
      VAST_DEBUG(self, "sets accountant to", accountant);
      auto& st = self->state;
      st.accountant = std::move(accountant);
      self->send(st.accountant, announce_atom::value, st.name);
    },
  };
}

} // namespace vast::system
