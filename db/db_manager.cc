// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db_manager.h"
#include <iostream>

namespace tdchunk {

bool DBManager::OpenDBs(const std::vector<std::string>& db_paths, bool do_concat, float extract_thres) {
  bool success = true;
  for (const auto& db_path : db_paths) {
    DB* db = new DB();
    success = db->Open(db_path, do_concat, extract_thres);
    _dbs.push_back(db);
  }
  return success;
}

// void DBManager::Flush(int index, const std::string& file_name) {
//   _dbs[index]->NotifyFlush(file_name);
// }

void DBManager::Join(int index, const std::vector<uint32_t>& keys, uint64_t file_number, uint64_t length) {
  _dbs[index]->NotifyJoin(keys, file_number, length);
}

std::vector<CkptMetaData> DBManager::GetCheckpointFiles(int index, int version) {
  return _dbs[index]->GetCheckpointFiles(version);
}

void DBManager::DeleteCheckpointsBefore(int index, int version) {
  _dbs[index]->DeleteCheckpointsBefore(version);
}

void DBManager::ReleaseDBs() {
  for (int i = 0; i < _dbs.size(); i++) {
    delete _dbs[i];
  }
  _dbs.clear();
}

uint64_t DBManager::GetNextNumber(int index) {
  return _dbs[index]->GetNextNumber();
}

void DBManager::PrintTree(int i) {
  _dbs[i]->PrintTree();
}

};