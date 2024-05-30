// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <msgpack.hpp>
#include <string>
#include <fstream>
#include <map>

void inline UnpackFile(const std::string& file_name, msgpack::object& obj) {
  std::ifstream input(file_name, std::ios::binary);
  std::stringstream buffer;
  buffer << input.rdbuf();

  // unpack MessagePack data
  msgpack::object_handle oh = msgpack::unpack(buffer.str().data(), buffer.str().size());
  obj = oh.get();
  // input.close();
}

uint32_t inline PackToFile(const std::string& file_name, std::map<uint32_t, std::vector<double>>& data_map) {
  std::stringstream buffer;
  msgpack::pack(buffer, data_map);
  std::ofstream file(file_name, std::ios::binary);
  file.write(buffer.str().data(), buffer.str().size());
  uint32_t length = file.tellp();
  file.close();
  return length;
}


void inline PackToFile(const std::string& file_name, std::map<uint32_t, msgpack::object>& data_map) {
  std::stringstream buffer;
  std::map<uint32_t, std::vector<double>> to_pack;
  for (auto data : data_map) {
    std::vector<double> tensor;
    data.second.convert(tensor);
    to_pack[data.first] = tensor;
  }
  msgpack::pack(buffer, to_pack);
  std::ofstream file(file_name, std::ios::binary);
  file.write(buffer.str().data(), buffer.str().size());
  file.close();
}
