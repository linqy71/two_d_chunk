// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>

#include "db_manager.h"

namespace py = pybind11;
using namespace tdchunk;

std::unordered_map<std::string, std::vector<py::tuple>> GetCheckpointFiles(DBManager* db_manager, int index, int version) {
  std::vector<CkptMetaData> metadata = db_manager->GetCheckpointFiles(index, version);
  std::unordered_map<std::string, std::vector<py::tuple>> res;
  for (auto meta : metadata) {
    res[meta.file_name].push_back(py::make_tuple(meta.start, meta.length));
  }
  return res;
}


PYBIND11_MODULE(py_tdchunk, m) {
  m.doc() = "tdchunk interface";

  py::class_<DBManager>(m, "DBManager")
      .def(py::init<>())
      .def("open", (bool (DBManager::*)(const std::vector<std::string>& db_paths, bool do_concat_, float extract_thres)) & DBManager::OpenDBs)
      // .def("flush", (bool (DBManager::*)(int index, const std::string& file_name)) & DBManager::Flush)
      .def("join", (void (DBManager::*)(int index, const std::vector<uint32_t>& keys, uint64_t file_number, int length)) & DBManager::Join)
      .def("get_next_number", (uint64_t (DBManager::*)(int index)) & DBManager::GetNextNumber)
      .def("delversion", (bool (DBManager::*)(int index, int version)) & DBManager::DeleteCheckpointsBefore)
      .def("releasedb", (void (DBManager::*)()) & DBManager::ReleaseDBs);

  m.def("getversion", &GetCheckpointFiles);

}