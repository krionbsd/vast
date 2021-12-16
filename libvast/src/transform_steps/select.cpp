//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/select.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/type.h>
#include <caf/expected.hpp>

namespace vast {

select_step::select_step(std::string expr)
  : expression_(caf::none) { // FIXME: Is caf::none OK?
  auto e = to<vast::expression>(std::move(expr));
  if (!e) {
    expression_ = e;
    return;
  }
  // FIXME: Is normalize useful here?
  expression_ = normalize_and_validate(*e);
}

caf::expected<table_slice> select_step::operator()(table_slice&& slice) const {
  if (!expression_)
    return expression_.error(); // FIXME: Is this message OK?
  auto tailored_expr = tailor(*expression_, slice.layout());
  if (!tailored_expr)
    return tailored_expr.error();
  auto new_slice = filter(slice, *tailored_expr);
  if (new_slice)
    return *new_slice;
  // FIXME: return an empty slice or caf::error if new_slice is empty?
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::table_slice_encoding::msgpack, slice.layout());
  return builder->finish();
}

class select_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "select";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto expr = caf::get_if<std::string>(&opts, "expression");
    if (!expr)
      return caf::make_error(ec::invalid_configuration,
                             "key 'expression' is missing or not a string in "
                             "configuration for select step");
    return std::make_unique<select_step>(*expr);
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::select_step_plugin)
