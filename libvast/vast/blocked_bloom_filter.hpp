//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository:       https://github.com/FastFilter/fastfilter_cpp
// - Commit:           95b7c98e805ee028a0934262d56e54f45f39ace7
// - Copyright Holder: Apache Software Foundation
// - Path:             src/bloom/simd-block-fixed-fpp.h
// - Created:          May 1, 2019
// - License:          Apache 2.0
//
// Adapted and modernized to fit in the VAST code base.

#pragma once

#include "vast/bloom_filter_parameters.hpp"
#include "vast/concept/hashable/uhash.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"

#include <caf/meta/type_name.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <utility>

#ifdef __AVX2__
#  include <x86intrin.h>
#else
#  error "Platform does not support AVX2 instructions"
#endif

// TODO:
// - use a SIMD library to avoid architecture-dependent instructions, e.g.,
//   xsimd, highway, or std::experimental::simd where available.
// - support ARM
namespace vast {

namespace detail {

template <class T>
struct delete_aligned {
  void operator()(T* x) const {
    std::free(x);
  }
};

template <class T>
std::unique_ptr<T[], delete_aligned<T>>
allocate_aligned(size_t alignment, size_t size) {
  auto ptr = std::aligned_alloc(alignment, size);
  return {static_cast<T*>(ptr), delete_aligned<T>{}};
}

} // namespace detail

/// A cache-efficient Bloom filter implementation. A blocked Bloom filter
/// consists of a sequence of small Bloom filters, each of which fits into one
/// cache line. This design allows for less than two cache misses on average,
/// unlike standard Bloom filters performing *k* random memory accesses.
///
/// The implementation is a slightly tuned version by Jim Appl, per the
/// following papers:
/// - https://arxiv.org/pdf/2101.01719.pdf
/// - https://arxiv.org/pdf/2109.01947.pdf
///
/// @tparam HashFunction The hash function to use.
template <class HashFunction>
class blocked_bloom_filter
  : detail::equality_comparable<blocked_bloom_filter<HashFunction>> {
  static_assert(sizeof(typename HashFunction::result_type) == sizeof(uint64_t));

public:
  using hash_function = HashFunction;
  using hash = uhash<hash_function>;

  using block_type = std::array<uint32_t, 8>; // __m256i
  static constexpr size_t block_size = sizeof(block_type);

  /// Constructs a blocked Bloom filter with a fixed size and a hash function.
  /// @param size The number of cells/bits in the Bloom filter.
  explicit blocked_bloom_filter(size_t size = 0)
    : num_blocks_{num_blocks(size)} {
    auto buffer_size = num_blocks_ * block_size;
    blocks_ = detail::allocate_aligned<block_type>(64, buffer_size);
    std::memset(blocks_.get(), 0, buffer_size);
    // TODO: there's probably a better way to do this.
    VAST_ASSERT(__builtin_cpu_supports("avx2"));
  }

  /// Adds an element to the Bloom filter.
  /// @param x The element to add.
  template <class T>
  [[gnu::always_inline]] inline void add(T&& x) {
    const uint64_t digest = hash{}(std::forward<T>(x));
    const uint32_t idx = block_index(digest, num_blocks_);
    auto blocks = reinterpret_cast<__m256i*>(blocks_.get());
    auto* block = &blocks[idx];
    // Perform a bitwise OR into the chosen block.
    _mm256_store_si256(block, _mm256_or_si256(*block, make_mask(digest)));
  }

  /// Test whether an element exists in the Bloom filter.
  /// @param x The element to test.
  /// @returns `false` if the *x* is not in the set and `true` if *x* may exist
  ///          according to the false-positive probability of the filter.
  template <class T>
  [[gnu::always_inline]] inline bool lookup(T&& x) const {
    const uint64_t digest = hash{}(std::forward<T>(x));
    const uint32_t idx = block_index(digest, num_blocks_);
    auto blocks = reinterpret_cast<__m256i*>(blocks_.get());
    auto* block = &blocks[idx];
    // Check that all bits in the block are set.
    return _mm256_testc_si256(*block, make_mask(digest));
  }

  // -- concepts --------------------------------------------------------------

  friend bool
  operator==(const blocked_bloom_filter& x, const blocked_bloom_filter& y) {
    VAST_ASSERT(x.blocks_ != nullptr);
    VAST_ASSERT(y.blocks_ != nullptr);
    return x.num_blocks_ == y.num_blocks_
           && std::memcmp(x.blocks_.get(), y.blocks_.get(),
                          x.num_blocks_ * block_size);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, blocked_bloom_filter& x) {
    return f(caf::meta::type_name("blocked_bloom_filter"), x.num_blocks_,
             x.blocks_);
  }

private:
  /// Computes the number of blocks for a given number of bits.
  static constexpr size_t num_blocks(size_t bits) {
    constexpr auto c = 24; // taken from FastFilter benchmark
    static_assert(c <= block_size);
    return std::max(size_t{1}, bits / c);
  }

  // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction
  static inline uint32_t reduce(uint32_t hash, uint32_t n) {
    return (uint32_t)(((uint64_t)hash * n) >> 32);
  }

  static inline uint64_t rotl64(uint64_t n, unsigned int c) {
    auto char_bit = std::numeric_limits<unsigned char>::digits;
    const unsigned int mask = (char_bit * sizeof(n) - 1);
    c &= mask;
    return (n << c) | (n >> ((-c) & mask));
  }

  static inline uint64_t block_index(uint64_t digest, size_t num_blocks) {
    return reduce(rotl64(digest, 32), num_blocks);
  }

  // Computes a mask with 1-bits to be set/checked in each 32-bit lane.
  [[gnu::always_inline]] static inline __m256i
  make_mask(const uint32_t digest) noexcept {
    const __m256i ones = _mm256_set1_epi32(1);
    // Set eight odd constants for multiply-shift hashing
    const __m256i rehash
      = _mm256_setr_epi32(0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                          0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U);
    __m256i digest_data = _mm256_set1_epi32(digest);
    digest_data = _mm256_mullo_epi32(rehash, digest_data);
    // Shift all data right, reducing the hash values from 32 bits to five bits.
    // Those five bits represent an index in [0, 31).
    digest_data = _mm256_srli_epi32(digest_data, 32 - 5);
    // Set a bit in each lane based on using the [0, 32) data as shift values.
    return _mm256_sllv_epi32(ones, digest_data);
  }

  size_t num_blocks_;
  std::unique_ptr<block_type[], detail::delete_aligned<block_type>> blocks_;
};

} // namespace vast
