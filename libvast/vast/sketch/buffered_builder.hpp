//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/sketch/builder.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstdint>
#include <unordered_set>

namespace vast::sketch {

/// The base class for buffered sketch builders. Derived classes must implement
/// the `build` function that performs a one-shot construction of the sketch.
class buffered_builder : public builder {
public:
  caf::error add(table_slice x, offset off) final;

  caf::expected<sketch> finish() final;

  /// Constructs a sketch using master digests.
  virtual caf::expected<sketch>
  build(const std::unordered_set<uint64_t>& digests) = 0;

private:
  std::unordered_set<uint64_t> digests_;
};

} // namespace vast::sketch
