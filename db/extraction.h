// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <vector>
#include <map>
#include <msgpack.hpp>
#include <fstream>
#include "file_list.h"

namespace tdchunk {

class Extraction {
public:
  // Files produced by extraction
  struct Output {
    uint64_t number = 0;
    uint32_t smallest, largest;
    uint64_t start, length;
  };

  Output retained;
  Output extracted;

  explicit Extraction(FileMetaData* base,
    const std::vector<FileMetaData*>& to_be_extracted)
    : base_(base),
      inputs_(to_be_extracted) {}

  ~Extraction() {
  }

  FileMetaData* base_;
  std::vector<FileMetaData*> inputs_;
  // std::vector<Output> outputs;

  // State kept for output being generated
  std::map<uint32_t, std::vector<double>> out_retained;
  std::map<uint32_t, std::vector<double>> out_extracted;

  // TODO
  // FilterBlockBuilder* filter_for_retained_file;

  std::vector<FileMetaData*> should_del_files;

};

}
