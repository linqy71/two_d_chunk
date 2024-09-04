// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "db.h"

namespace tdchunk {

class DBManager {
 public:
  bool OpenDBs(const std::vector<std::string>& db_paths, bool do_concat, float extract_thres);

  void Flush(int index, const std::string& file_name);

  void Join(int index, const std::vector<uint32_t>& keys, uint64_t file_number, uint64_t length);

  std::vector<CkptMetaData> GetCheckpointFiles(int index, int version);

  void DeleteCheckpointsBefore(int index, int version);

  void ReleaseDBs();

  void PrintTree(int index);
  uint64_t GetNextNumber(int index);

 private:
  std::vector<DB*> _dbs;

};

}
