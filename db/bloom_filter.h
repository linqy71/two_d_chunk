// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include "util/coding.h"

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif

namespace tdchunk {

uint32_t Hash(const uint32_t& data, uint32_t seed);
static uint32_t BloomHash(const uint32_t& key);

class BloomFilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key);

  void CreateFilter(const std::vector<uint32_t>& keys, std::string* dst) const;

  bool KeyMayMatch(const uint32_t& key, const char* bloom_filter, int n) const;

 private:
  size_t bits_per_key_;
  size_t k_;
};

}  // namespace tdchunk
