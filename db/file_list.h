// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>
#include <fstream>
#include <unordered_set>

#include "util/coding.h"
#include "file_helper.h"

namespace tdchunk{

class FileLinkedList {
 public:
  explicit FileLinkedList(std::vector<FileMetaData*>& list);
  ~FileLinkedList();

  struct FileListNode {
    FileMetaData* file_;
    FileListNode* next_;
  };

  // a list node without file metadata
  struct L0_ListNode{
    L0_ListNode(FileMetaData* file, L0_ListNode* next);
    ~L0_ListNode();
    FileListNode* children_head_; // 纵向的，包含所有被剥离出来的文件
    int num_of_children; // StripptionList长度（包含位于L0的文件本身）
    int num_of_empty_children;
    L0_ListNode* next_;
  };

  void AddL0Node(FileMetaData* file);

  bool ReplaceL0Node(FileMetaData* file, int column);

  bool ExtractOneChild(FileMetaData* child, int column);

  void MoveOtherToDeeper(std::unordered_set<uint64_t>& input_file_columns, std::vector<FileMetaData*>& file_list);
  void MoveChildrenToDeeperLevel(L0_ListNode* cur, std::vector<FileMetaData*>& file_list); // start from start_node

  bool GetVersion(int n, std::vector<FileMetaData*>& results);
  void DeleteVersion(std::string dbname_, int n, std::vector<FileMetaData*>& should_delete);

  bool GetOverlappedFilesL0(std::vector<FileMetaData*>& results);

  FileMetaData* getHeadFileMeta();

  uint64_t NextFileNumber();

  void PrintList();
  std::vector<std::vector<FileMetaData*>> MergeColumns(int start, int end);
  bool ShouldMerge(int& start, int& end, int every);

  uint64_t max_file_num_;
 private:

  
  L0_ListNode* l0_head;
  
};

}

