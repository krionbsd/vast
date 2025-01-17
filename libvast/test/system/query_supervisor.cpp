//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE query_supervisor

#include "vast/system/query_supervisor.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/query.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

using namespace vast;

namespace {

system::partition_actor::behavior_type
dummy_partition(system::partition_actor::pointer self, ids x) {
  return {
    [=](const vast::query& q) {
      auto sink = caf::get<query::count>(q.cmd).sink;
      self->send(sink, rank(x));
      return atom::done_v;
    },
    [=](atom::erase) -> atom::done {
      FAIL("dummy implementation not available");
    },
    [=](atom::status, system::status_verbosity) {
      return record{};
    },
  };
}

class fixture : public fixtures::deterministic_actor_system {
public:
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(query_supervisor_tests, fixture)

TEST(lookup) {
  MESSAGE("spawn supervisor, it should register itself as a worker on launch");
  auto sv
    = sys.spawn(system::query_supervisor,
                caf::actor_cast<system::query_supervisor_master_actor>(self));
  run();
  expect((atom::worker, system::query_supervisor_actor),
         from(sv).to(self).with(atom::worker_v, sv));
  MESSAGE("spawn partitions");
  auto p0 = sys.spawn(dummy_partition, make_ids({0, 2, 4, 6, 8}));
  auto p1 = sys.spawn(dummy_partition, make_ids({1, 7}));
  auto p2 = sys.spawn(dummy_partition, make_ids({3, 5}));
  run();
  MESSAGE("fill query map and trigger supervisor");
  auto query_id = uuid::random();
  system::query_map qm{
    {uuid::random(), p0}, {uuid::random(), p1}, {uuid::random(), p2}};
  self->send(sv, atom::supervise_v, query_id,
             vast::query::make_count(self, query::count::mode::estimate,
                                     unbox(to<expression>("x == 42"))),
             std::move(qm),
             caf::actor_cast<system::receiver_actor<atom::done>>(self));
  run();
  MESSAGE("collect results");
  bool done = false;
  uint64_t result = 0;
  while (!done)
    self->receive([&](const uint64_t& x) { result += x; },
                  [&](atom::done) { done = true; });
  CHECK_EQUAL(result, 9u);
  MESSAGE("after completion, the supervisor should register itself again");
  expect((atom::worker, system::query_supervisor_actor),
         from(sv).to(self).with(atom::worker_v, sv));
}

FIXTURE_SCOPE_END()
