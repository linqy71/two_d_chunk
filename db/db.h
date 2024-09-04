// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <thread>

#include "extraction.h"
#include "file_list.h"
#include "bloom_filter.h"

namespace tdchunk {

class MemTable;

class DB {
 public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // true on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  bool Open(const std::string& name, bool do_concat, float extract_thres);

  DB();
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  ~DB();

  void NotifyJoin(const std::vector<uint32_t>& keys, uint64_t file_number, uint64_t length);
  // void Flush();
  void Join(const std::vector<uint32_t>& keys, uint64_t file_number, uint64_t length);

  std::vector<CkptMetaData> GetCheckpointFiles(int version);

  void DeleteCheckpointsBefore(int version);

  bool ShouldExtract(const std::vector<uint32_t>& keys, std::vector<FileMetaData*>& to_be_extracted);

  void PrintTree();

  void Merge(int start, int end);
  uint64_t GetNextNumber();

 private:

  void BackgroundExtraction(const std::vector<uint32_t>& keys);

  bool DoExtractionWork(Extraction* e); // args to be decided 

  bool InstallExtractionResults(Extraction* extract, int column);

  bool RewriteManifest();

  // delete unuseful files
  bool CleanupExtraction(Extraction* e);

  void CreateFilterForMap(const std::map<int32_t, std::vector<double>>& data_map);

  std::ofstream manifest_;
  std::ofstream filter_file_;
  std::string dbname_;
  bool use_filter_;
  BloomFilterPolicy* filter_policy_;

  // use to sync main thread and sub thread
  bool background_compaction_scheduled_;

  // file_num -> ref
  std::unordered_map<uint64_t, int> merged_file_ref;
  std::vector<FileMetaData*>* file_list_;
  FileLinkedList* file_linked_list;
  std::unique_ptr<std::thread> bg_flush;
  std::string cur_flush_file;
  bool do_concat_;
  // threshold of the number of kvs to be extracted
  float extract_thres_;

};
}
