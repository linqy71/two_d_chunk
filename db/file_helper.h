// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>
#include <fstream>
#include <vector>

#include "util/coding.h"

namespace tdchunk{

enum Tag {
  kDeletedFile = 0,
  kNewFile = 1,
  kFlag = 2, // for empty nodes 
  kMergedFile = 3, // for merged nodes
  kMergedRef = 4 // for referece counters
};
struct FileMetaData {
  uint32_t tag;
  uint64_t start;
  uint64_t length;
  uint32_t level;
  uint32_t column; // which node
  uint64_t number;
  uint32_t smallest;  // Smallest key 
  uint32_t largest;   // Largest key
  uint64_t filter_start = 0;
  uint64_t filter_length = 0;  
};

struct CkptMetaData {
  std::string file_name;
  uint64_t start;
  uint64_t length;
};

bool CompareFileMetaData(FileMetaData* a, FileMetaData* b);
int CompareKey(const std::string& a, const std::string& b);
std::string MakeFileName(const std::string& dbname, uint64_t number,
                                const char* suffix);

bool CreateDir(const std::string& dirname);
bool DeleteFile(const std::string& filename);
bool FileExists(const std::string& filename);
bool GetChildren(const std::string& directory_path,
                    std::vector<std::string>* result);

void EncodeTo(FileMetaData* file, std::string& dst);

}

