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

#include <caf/all.hpp>

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/fwd.hpp"

#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

namespace fixtures {

using namespace vast;

struct events {
  events();

  /// Maximum size of all generated slices.
  static size_t slice_size;

  static std::vector<event> bro_conn_log;
  static std::vector<event> bro_dns_log;
  static std::vector<event> bro_http_log;
  static std::vector<event> bgpdump_txt;
  static std::vector<event> random;

  static std::vector<table_slice_ptr> bro_conn_log_slices;
  // TODO: table_slice::recursive_add flattens too much, why the following
  //       slices won't work. However, flatten(value) is also broken
  //       at the moment (cf. #3215), so we can't fix it until then.
  static std::vector<table_slice_ptr> bro_http_log_slices;
  static std::vector<table_slice_ptr> bro_dns_log_slices;
  static std::vector<table_slice_ptr> bgpdump_txt_slices;
  // static std::vector<table_slice_ptr> random_slices;

  /// 10000 ascending integer values, starting at 0.
  static std::vector<event> ascending_integers;
  static std::vector<table_slice_ptr> ascending_integers_slices;

  /// 10000 integer values, alternating between 0 and 1.
  static std::vector<event> alternating_integers;
  static std::vector<table_slice_ptr> alternating_integers_slices;

  static record_type bro_conn_log_layout();

  template <class... Ts>
  static std::vector<vector> make_rows(Ts... xs) {
    return {make_vector(xs)...};
  }

  std::vector<table_slice_ptr> copy(std::vector<table_slice_ptr> xs);

private:
  template <class Reader>
  static std::vector<event> inhale(const char* filename) {
    auto input = std::make_unique<std::ifstream>(filename);
    Reader reader{std::move(input)};
    return extract(reader);
  }

  template <class Reader>
  static std::vector<event> extract(Reader&& reader) {
    auto e = expected<event>{no_error};
    std::vector<event> events;
    while (e || !e.error()) {
      e = reader.read();
      if (e)
        events.push_back(std::move(*e));
    }
    REQUIRE(!e);
    CHECK(e.error() == ec::end_of_input);
    REQUIRE(!events.empty());
    return events;
  }
};

} // namespace fixtures
