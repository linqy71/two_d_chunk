// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <thread>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <map>

#include "msgpack_helper.h"

#include "db.h"
#include "file_helper.h"

namespace tdchunk {

DB::DB()
  : use_filter_(true),
    background_compaction_scheduled_(false) {
  file_linked_list = nullptr;
  file_list_ = new std::vector<FileMetaData*>();
  bg_flush = nullptr;
  if (use_filter_) {
    filter_policy_ = new BloomFilterPolicy(16);
  }
}

DB::~DB() {
  if (bg_flush && bg_flush->joinable()) {
    bg_flush->join();
  }
  if (filter_file_.is_open()) {
    filter_file_.flush();
    filter_file_.close();
  }
  if (manifest_.is_open()) {
    manifest_.flush();
    manifest_.close();
  }

  delete file_linked_list;

  // delete file_list and filemetadata
  for (auto it = file_list_->begin(); it != file_list_->end(); it++) {
    delete *it;
  }
  delete file_list_;

}


bool DB::Open(const std::string& name, bool do_concat, float extract_thres) {
  // open db
  dbname_ = name;
  do_concat_ = do_concat;
  extract_thres_ = extract_thres;

  bool s = CreateDir(dbname_);
  std::string manifest_name = dbname_ + "/manifest";
  //filter
  std::string filter_file_name_ = dbname_ + "/filter";
  if (use_filter_) {
    if (!FileExists(filter_file_name_)) {
      filter_file_.open(filter_file_name_, std::ios::out | std::ios::trunc);
    } else {
      filter_file_.open(filter_file_name_, std::ios::out | std::ios::app);
    }
  }
  //manifest
  if (!FileExists(manifest_name)) {
    manifest_.open(manifest_name, std::ios::out | std::ios::trunc); //create new file
  } else {
    // recover db according to manifest
    std::ifstream file(manifest_name);
    file.seekg(0, std::ios::beg);

    uint32_t tag, level, column;
    uint64_t start;
    uint64_t length;
    uint64_t number;
    uint32_t smallest, largest;
    uint64_t filter_start, filter_length;
    while(file >> tag) {
      FileMetaData* f = new FileMetaData;
      if (tag == kFlag) {
        file >> level >> column;
        f->tag = tag;
        f->level = level;
        f->column = column;
        f->number = 0;
        file_list_->push_back(f);
      } else if (tag == kNewFile || tag == kMergedFile) {
        file >> start >> length >> level >> column >> number >> smallest >> largest >> filter_start >> filter_length;
        f->tag = tag;
        f->start = start;
        f->length = length;
        f->level = level;
        f->column = column;
        f->number = number;
        f->smallest = smallest;
        f->largest = largest;
        f->filter_start = filter_start;
        f->filter_length = filter_length;
        file_list_->push_back(f);
      } else if (tag == kMergedRef) { // to delete merged file
        int ref;
        file >> number >> ref;
        if (ref != 0) {
          merged_file_ref[number] = ref;
        }
      }
      
    }
    file.close();
    manifest_.open(manifest_name, std::ios::out | std::ios::app);

  }

  // recover linked list
  file_linked_list = new FileLinkedList(*file_list_);

  return true;
}

void DB::NotifyJoin(const std::vector<uint32_t>& keys, uint64_t file_number, uint64_t length) {
  if (bg_flush && bg_flush->joinable()) {
    bg_flush->join();
  }
  // bg_flush.reset(new std::thread(&DB::Flush, this));
  bg_flush.reset(new std::thread(std::bind(&DB::Join, this, keys, file_number, length)));
  // Flush();
}

uint64_t DB::GetNextNumber() {
  return file_linked_list->NextFileNumber();
}

void DB::Join(const std::vector<uint32_t>& keys, uint64_t file_number, uint64_t length) {
  uint64_t filter_start = 0, filter_length = 0;
  if(use_filter_) {
    std::string result;
    filter_policy_->CreateFilter(keys, &result);
    filter_start = filter_file_.tellp();
    filter_length = result.length();
    filter_file_ << result;
    filter_file_.flush();
  }
  //create metadata
  FileMetaData* meta = new FileMetaData();
  meta->number = file_number;
  meta->smallest = keys[0];
  meta->largest = keys[keys.size() - 1];
  meta->tag = kNewFile;
  meta->level = 0;
  meta->filter_start = filter_start;
  meta->filter_length = filter_length;
  meta->start = 0;
  meta->length = length;

  file_list_->push_back(meta);
  file_linked_list->AddL0Node(meta);

  std::string to_write;
  EncodeTo(meta, to_write);
  manifest_ << to_write;
  manifest_.flush();

  BackgroundExtraction(keys);
}

// void DB::Flush() {
//   auto file_name = cur_flush_file;
//   msgpack::object obj;
//   std::ifstream input(file_name, std::ios::binary);
//   input.seekg(0, std::ios::beg);
//   std::stringstream buffer;
//   buffer << input.rdbuf();

//   // unpack MessagePack data
//   msgpack::object_handle oh = msgpack::unpack(buffer.str().data(), buffer.str().size());
//   obj = oh.get();
//   // std::cout << obj << std::endl;

//   // check obj type
//   if (obj.type != msgpack::type::MAP) {
//       std::cerr << "Data is not a dictionary/map!" << std::endl;
//       return ;
//   }

//   // convert to map, python list is coverted to vector<double>
//   std::map<uint32_t, std::vector<double>> data_map;
//   obj.convert(data_map);

//   uint64_t filter_start = 0, filter_length = 0;
//   std::vector<uint32_t> keys;
//   //create filter
//   if(use_filter_) {
//     for(auto item : data_map) {
//       keys.push_back(item.first);
//     }
//     std::string result;
//     filter_policy_->CreateFilter(keys, &result);
//     filter_start = filter_file_.tellp();
//     filter_length = result.length();
//     filter_file_ << result;
//     filter_file_.flush();
//   }

//   //create metadata
//   FileMetaData* meta = new FileMetaData();
//   meta->number = file_linked_list->NextFileNumber();
//   meta->smallest = data_map.begin()->first;
//   meta->largest = data_map.rbegin()->first;
//   meta->tag = kNewFile;
//   meta->level = 0;
//   meta->filter_start = filter_start;
//   meta->filter_length = filter_length;

//   // std::cout << "flushing " << meta->smallest << ", " << meta->largest << std::endl;

//   file_list_->push_back(meta);
//   file_linked_list->AddL0Node(meta);

//   std::string fname = MakeFileName(dbname_, meta->number, "tdc");
//   uint32_t length = PackToFile(fname, data_map);
//   meta->start = 0;
//   meta->length = length;
  
//   input.close();
//   //remove input file
//   DeleteFile(file_name);

//   std::string to_write;
//   EncodeTo(meta, to_write);
//   manifest_ << to_write;
//   manifest_.flush();

//   BackgroundExtraction(keys);

// }

bool DB::ShouldExtract(const std::vector<uint32_t>& keys, std::vector<FileMetaData*>& to_be_extracted) {
  to_be_extracted.clear();
  if (extract_thres_ > 0 && keys.size() <= 100) return false;

  auto thres = static_cast<int>(keys.size() * extract_thres_);
  
  //first, choose files by smallest and largest key
  std::vector<FileMetaData*> overlapped;
  file_linked_list->GetOverlappedFilesL0(overlapped);
  if (overlapped.size() == 0) return false;

  if (!use_filter_) {
    to_be_extracted = overlapped;
  } else {
    //open filter file for read;
    std::ifstream f(dbname_ + "/filter");
    assert(!keys.empty());

    for (auto file : overlapped) {
      if (file->filter_length == 0) {
        continue;
      }
      f.seekg(file->filter_start, std::ios::beg);
      char* filter = new char[file->filter_length];
      f.read(filter, file->filter_length);

      int hit = 0;
      for (auto key : keys) {
        bool e = filter_policy_->KeyMayMatch(key, filter, file->filter_length);
        hit += e;
      }
      if (hit > thres) {
        // std::cout << hit << " ";
        to_be_extracted.push_back(file);
      }

      delete filter;
    }
    // std::cout << std::endl;
    f.close();
  }

  return to_be_extracted.size() != 0;
}

void DB::BackgroundExtraction(const std::vector<uint32_t>& keys) {
  bool rewrite = false;
  std::vector<FileMetaData*> input;
  if (ShouldExtract(keys, input)) {// generate Extraction
    // std::cout << "doing extraction with " << input.size() << " files" << std::endl;
    
    Extraction* e = new Extraction(file_linked_list->getHeadFileMeta(), input);
    bool success = DoExtractionWork(e);

    rewrite = true;

    if (success) {
      CleanupExtraction(e);
    } else {
      assert(success); // error 
    }
    delete e;
  }
  if (rewrite) {
    RewriteManifest();
  }
  return ;
}

bool DB::RewriteManifest() {
  //write manifest
  manifest_.close();
  manifest_.open(dbname_+"/manifest", std::ios::out | std::ios::trunc);
  for (auto file : *file_list_) {
    if (file->tag == kDeletedFile) continue;
    std::string to_write;
    EncodeTo(file, to_write);
    manifest_ << to_write;
  }
  for (auto pair : merged_file_ref) {
    std::string to_write = std::to_string(kMergedRef) + " "
                          + std::to_string(pair.first) + " "
                          + std::to_string(pair.second) + "\n";
    manifest_ << to_write;
  }
  manifest_.flush();

}

bool DB::CleanupExtraction(Extraction* e) {
  // delete input files
  bool success = true;
  for (auto file : e->should_del_files) {
    if (file->tag == kNewFile) {
      auto fname = MakeFileName(dbname_, file->number, "tdc");
      success = DeleteFile(fname);
      file->tag = kDeletedFile;
    } else if (file->tag == kMergedFile) {
      merged_file_ref[file->number]--;
      if (merged_file_ref[file->number] <= 0) {
        auto fname = MakeFileName(dbname_, file->number, "tdc");
        success = DeleteFile(fname);
        merged_file_ref.erase(file->number);
      }
      // file is not deleted but file meta should be deleted
      file->tag = kDeletedFile;
    }
    if (!success) return success;
  }

  return success;
}


bool DB::DoExtractionWork(Extraction* e) {
  // 1. unpack base file
  std::string base_fname = MakeFileName(dbname_, e->base_->number, "tdc");
  msgpack::object base_file_content;
  std::ifstream base_input(base_fname, std::ios::binary);
  base_input.seekg(0, std::ios::beg);
  std::stringstream buffer;
  buffer << base_input.rdbuf();

  // unpack MessagePack data
  msgpack::object_handle oh = msgpack::unpack(buffer.str().data(), buffer.str().size());
  base_file_content = oh.get();

  std::map<uint32_t, std::vector<double>> base_map;
  base_file_content.convert(base_map);

  assert(!base_map.empty());
  // generate base file iterator
  auto base_file_iter = base_map.begin();

  std::unordered_set<uint64_t> input_file_columns;

  // 2. for every file in inputs
  int ext_cnt = 0;
  std::vector<int> act_files;
  std::ofstream concated_retained_file_;
  std::ofstream concated_extracted_file_;
  for (auto file : e->inputs_) {
    // reset base_iter
    base_file_iter = base_map.begin();


    // find equal keys using double pointer
    std::string fname = MakeFileName(dbname_, file->number, "tdc");
    msgpack::object cur_file_content;
    std::ifstream cur_input(fname, std::ios::binary);
    cur_input.seekg(file->start, std::ios::beg);
    char* cur_buffer = new char[file->length];
    cur_input.read(cur_buffer, file->length);
    // unpack MessagePack data
    msgpack::object_handle oh = msgpack::unpack(cur_buffer, file->length);
    cur_file_content = oh.get();
    
    std::map<uint32_t, std::vector<double>> cur_map;
    cur_file_content.convert(cur_map);

    assert(!cur_map.empty());

    auto cur_file_iter = cur_map.begin();

    int comp = file->smallest - e->base_->smallest;

    if (comp > 0) { // comp > 0
      // base iter seek to file->smallest
      for (; base_file_iter != base_map.end(); base_file_iter++) {
        if (base_file_iter->first >= file->smallest) {
          break;
        }
      }
      if (base_file_iter->first > file->largest) {
        cur_input.close();
        delete cur_buffer;
        continue;
      }
    }

    int total_extracted = 0;
    while (cur_file_iter != cur_map.end() && base_file_iter != base_map.end()) {
      
      bool extracted = false;
      bool retained = false;
      auto f_key = cur_file_iter->first;
      auto f_value = cur_file_iter->second;
      auto base_key = base_file_iter->first;
      int equal = f_key - base_key;
      if (equal > 0) {
        base_file_iter++;
      } else if (equal < 0) {
        retained = true;
        cur_file_iter++;
      } else { //equal, extract to file
        extracted = true;
        total_extracted++;
        cur_file_iter++;
        base_file_iter++;
      }

      // push into data_map
      if (extracted) {
        e->out_extracted[f_key] = f_value;
      }
      if (retained) {
        e->out_retained[f_key] = f_value;
      }
    }
    
    while (cur_file_iter != cur_map.end()) { // write unfinished keys
      auto f_key = cur_file_iter->first;
      auto f_value = cur_file_iter->second;
      e->out_retained[f_key] = f_value;
      cur_file_iter++;
    }


    //cur file done. Before switch to next file, save and reset data_map
    if (total_extracted <= static_cast<int>(base_map.size() * extract_thres_)) { 
      // no equal keys found or too little extracted data, should not extract file
      // do nothing and clear current data_map
      // assert(e->out_extracted.empty());
      e->out_retained.clear();
      // do not install
    } else {
      // std::cout << total_extracted << " ";
      ext_cnt++;
      act_files.push_back(file->column);
      
      if (do_concat_) {
        if (!concated_extracted_file_.is_open()) {
          e->extracted.number = file_linked_list->NextFileNumber();
          e->retained.number = file_linked_list->NextFileNumber();
          auto extracted_file = MakeFileName(dbname_, e->extracted.number, "tdc");
          concated_extracted_file_.open(extracted_file, std::ios::binary);
          auto retained_file = MakeFileName(dbname_, e->retained.number, "tdc");
          concated_retained_file_.open(retained_file, std::ios::binary);
          concated_extracted_file_.seekp(std::ios::beg);
          concated_retained_file_.seekp(std::ios::beg);
          merged_file_ref[e->extracted.number] = 0;
          merged_file_ref[e->retained.number] = 0;
        }
        assert(concated_extracted_file_.is_open());
        assert(concated_retained_file_.is_open());
        
        if (!e->out_extracted.empty()) {
          e->extracted.start = concated_extracted_file_.tellp();
          std::stringstream buffer;
          msgpack::pack(buffer, e->out_extracted);
          concated_extracted_file_.write(buffer.str().data(), buffer.str().size());
          e->extracted.length = buffer.str().size();
          e->extracted.smallest = e->out_extracted.begin()->first;
          e->extracted.largest = e->out_extracted.rbegin()->first;
          merged_file_ref[e->extracted.number]++;
        }
        if (!e->out_retained.empty()) {
          e->retained.start = concated_retained_file_.tellp();
          std::stringstream buffer;
          msgpack::pack(buffer, e->out_retained);
          concated_retained_file_.write(buffer.str().data(), buffer.str().size());
          e->retained.length = buffer.str().size();
          e->retained.smallest = e->out_retained.begin()->first;
          e->retained.largest = e->out_retained.rbegin()->first;
          merged_file_ref[e->retained.number]++;
        }
      } else {
        e->extracted.number = file_linked_list->NextFileNumber();
        e->retained.number = file_linked_list->NextFileNumber();
        auto extracted_file = MakeFileName(dbname_, e->extracted.number, "tdc");
        auto retained_file = MakeFileName(dbname_, e->retained.number, "tdc");

        if (!e->out_extracted.empty()) {
          auto length = PackToFile(extracted_file, e->out_extracted);
          e->extracted.smallest = e->out_extracted.begin()->first;
          e->extracted.largest = e->out_extracted.rbegin()->first;
          e->extracted.start = 0;
          e->extracted.length = length;
        }
        if (!e->out_retained.empty()) {
          auto length = PackToFile(retained_file, e->out_retained);
          e->retained.smallest = e->out_retained.begin()->first;
          e->retained.largest = e->out_retained.rbegin()->first;
          e->retained.start = 0;
          e->retained.length = length;
        }
      }

      InstallExtractionResults(e, file->column);
      e->out_extracted.clear();
      e->out_retained.clear();

      // move children to deeper
      input_file_columns.insert(file->column);

      e->should_del_files.push_back(file);
      // file->tag = kDeletedFile;
    }
    cur_input.close();
    delete cur_buffer;
  }
  base_input.close();
  file_linked_list->MoveOtherToDeeper(input_file_columns, *file_list_);
  if (do_concat_) {
    if (concated_extracted_file_.is_open()) {
      concated_extracted_file_.flush();
      concated_extracted_file_.close();
    }
    if (concated_retained_file_.is_open()) {
      concated_retained_file_.flush();
      concated_retained_file_.close();
    }
  }

  return true;
}



bool DB::InstallExtractionResults(Extraction* extract, int column) {
  assert(extract != nullptr);
  // add to linked list
  if (!extract->out_retained.empty()) {

    FileMetaData* retained_meta = new FileMetaData();

    // create filter, just append, ignore unavailable filters
    if (use_filter_) {
      std::vector<uint32_t> keys;
      for (auto item : extract->out_retained) {
        keys.push_back(item.first);
      }
      std::string result;
      filter_policy_->CreateFilter(keys, &result);
      auto start = filter_file_.tellp();
      auto length = result.length();
      filter_file_ << result;
      filter_file_.flush();
      retained_meta->filter_start = start;
      retained_meta->filter_length = length;
    }
    if (do_concat_) {
      retained_meta->tag = kMergedFile;
    } else {
      retained_meta->tag = kNewFile;
    }
    retained_meta->number = extract->retained.number;
    retained_meta->level = 0;
    retained_meta->column = column;
    retained_meta->smallest = extract->retained.smallest;
    retained_meta->largest = extract->retained.largest;
    retained_meta->start = extract->retained.start;
    retained_meta->length = extract->retained.length;
    file_linked_list->ReplaceL0Node(retained_meta, column);
    file_list_->push_back(retained_meta);
  } else {
    FileMetaData* retained_meta = new FileMetaData();
    retained_meta->tag = kFlag;
    retained_meta->number = extract->retained.number;
    retained_meta->level = 0;
    retained_meta->column = column;
    file_linked_list->ReplaceL0Node(retained_meta, column);
    file_list_->push_back(retained_meta);
  }
  if (!extract->out_extracted.empty()) {
    FileMetaData* extracted_meta = new FileMetaData();
    if (do_concat_) {
      extracted_meta->tag = kMergedFile;
    } else {
      extracted_meta->tag = kNewFile;
    }
    extracted_meta->number = extract->extracted.number;
    extracted_meta->smallest = extract->extracted.smallest;
    extracted_meta->largest = extract->extracted.largest;
    extracted_meta->start = extract->extracted.start;
    extracted_meta->length = extract->extracted.length;
    extracted_meta->level = 1;
    extracted_meta->column = column;
    file_linked_list->ExtractOneChild(extracted_meta, column);
    file_list_->push_back(extracted_meta);
  }

  return true;
}

// bool compare(const CkptMetaData& a, const CkptMetaData& b) {
//   if (a.file_name == b.file_name) {
//     return a.start < b.start;
//   }
//   return a.file_name < b.file_name;
// }

std::vector<CkptMetaData> DB::GetCheckpointFiles(int version) {
  // gm: when background_compaction_scheduled_ is true wait;
  // while (background_compaction_scheduled_) {
  //   continue;
  // }
  // assert(!background_compaction_scheduled_);

  std::vector<FileMetaData*> results;
  std::vector<CkptMetaData> ckpt_res;

  bool success = file_linked_list->GetVersion(version, results);
  if (success) {
    for (auto it : results) {
      if(it->tag == kFlag) {
        continue;
      } else if (it->tag == kNewFile || it->tag == kMergedFile) {
        auto number = it->number;
        auto name = MakeFileName(dbname_, number, "tdc");
        CkptMetaData cur_ckpt;
        cur_ckpt.file_name = name;
        cur_ckpt.start = it->start;
        cur_ckpt.length = it->length;
        ckpt_res.push_back(cur_ckpt);
      }
    }
  }
  // std::sort(ckpt_res.begin(), ckpt_res.end(), compare);
  return ckpt_res;
}

//delete versions that <= n
void DB::DeleteCheckpointsBefore(int version) {

  // while (background_compaction_scheduled_) {
  //   continue;
  // }
  // assert(!background_compaction_scheduled_);
  
  //1. remove nodes from file_linked_list and get files need to delete
  std::vector<FileMetaData* > should_delete;
  file_linked_list->DeleteVersion(dbname_, version, should_delete);

  //2. delete meta and remove from file_list
  for (auto meta : should_delete){
    if (meta->tag == kNewFile){
      auto fname = MakeFileName(dbname_, meta->number, "tdc");
      DeleteFile(fname);
    }
    meta->tag = kDeletedFile;
    file_list_->erase(std::remove(file_list_->begin(), file_list_->end(), meta), file_list_->end());
    delete meta;
  }
  //3. update manifest
  RewriteManifest();
  return ;
}

void DB::PrintTree() {
  file_linked_list->PrintList();
}

void DB::Merge(int start, int end) {
  std::vector<std::vector<FileMetaData*>> to_merge = file_linked_list->MergeColumns(start, end);
  if (to_merge.size() == 0) return;

  for (auto level_files : to_merge) {
    // open first file and merge other files to the first file 
    assert(level_files.size() > 1);
    auto file_name = MakeFileName(dbname_, level_files[0]->number, "tdc");
    std::ofstream file(file_name, std::ios::app | std::ios::binary);
    level_files[0]->tag = kMergedFile;
    level_files[0]->start = 0;
    level_files[0]->length = file.tellp();
    for (int i = 1; i < level_files.size(); i++) {
      assert(level_files[i]->tag == kNewFile);
      level_files[i]->tag = kMergedFile;
      level_files[i]->start = file.tellp();
      auto cur_name = MakeFileName(dbname_, level_files[i]->number, "tdc");
      std::ifstream cur_file(cur_name, std::ios::in | std::ios::binary);
      std::stringstream buffer;
      buffer << cur_file.rdbuf();
      file.write(buffer.str().data(), buffer.str().size());
      level_files[i]->length = (uint64_t)file.tellp() - level_files[i]->start;
      level_files[i]->number = level_files[0]->number;
      // delete old file
      cur_file.close();
      DeleteFile(cur_name);
    }

    // record ref count
    merged_file_ref[level_files[0]->number] = level_files.size();
  }
}

}