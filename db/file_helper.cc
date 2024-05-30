// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "file_helper.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

namespace tdchunk{

bool CompareFileMetaData(FileMetaData* a, FileMetaData* b) {
  if (a->column == b->column) {
    return a->level < b->level;
  }
  return a->column < b->column;
}

std::string MakeFileName(const std::string& dbname, uint64_t number,
                                const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}

bool CreateDir(const std::string& dirname) {
  if (::mkdir(dirname.c_str(), 0755) != 0) {
    return false;
  }
  return true;
}

bool FileExists(const std::string& filename) {
  return ::access(filename.c_str(), F_OK) == 0;
}

bool DeleteFile(const std::string& filename) {
  return ::unlink(filename.c_str()) == 0;
}


bool GetChildren(const std::string& directory_path,
                    std::vector<std::string>* result) {
  result->clear();
  ::DIR* dir = ::opendir(directory_path.c_str());
  if (dir == nullptr) {
    return false;
  }
  struct ::dirent* entry;
  while ((entry = ::readdir(dir)) != nullptr) {
    result->emplace_back(entry->d_name);
  }
  ::closedir(dir);
  return true;
}

void EncodeTo(FileMetaData* file, std::string& dst) {
  if (file->tag == kFlag) {
    dst = std::to_string(file->tag) + " " +
                          std::to_string(file->level) + " " +
                          std::to_string(file->column) + "\n";
  } else if (file->tag == kMergedFile || file->tag == kNewFile) {
    dst = std::to_string(file->tag) + " " +
                           std::to_string(file->start) + " " +
                           std::to_string(file->length) + " " + 
                           std::to_string(file->level) + " " +
                           std::to_string(file->column) + " " + 
                           std::to_string(file->number) + " " +
                           std::to_string(file->smallest) + " " + 
                           std::to_string(file->largest) +  " " +
                           std::to_string(file->filter_start) + " " +
                           std::to_string(file->filter_length) + "\n";
  } else {
    dst = std::to_string(file->tag) + " " +
                           std::to_string(file->level) + " " +
                           std::to_string(file->column) + " " + 
                           std::to_string(file->number) + " " +
                           std::to_string(file->smallest) + " " + 
                           std::to_string(file->largest) + "\n";
  }
}

}
