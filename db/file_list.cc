// Copyright (c) 2024 Qingyin Lin. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "file_list.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <algorithm>
#include <assert.h>
#include <iostream>

namespace tdchunk{

FileLinkedList::FileLinkedList(std::vector<FileMetaData*>& list) {

  int max_file_num = 0;
  std::sort(list.begin(), list.end(), &CompareFileMetaData);
  l0_head = nullptr;

  if (!list.empty()) {
    int cur_level = 0;
    int cur_column = 0;
    FileListNode* cur_child = nullptr;
    for (auto file : list) {
      //get max file number
      if (file->number > max_file_num) {
        max_file_num = file->number;
      }
      if (file->tag == kDeletedFile) continue; //ignore

      if (file->column != cur_column) {
        //switch to next column
        assert(file->column == (cur_column + 1) );
        cur_column = file->column;
        cur_level = 0;
      }
      if (file->level == 0) { // create l0 node
        assert(cur_level == 0);
        l0_head = new L0_ListNode(file, l0_head);
        cur_level = 0;
        cur_child = l0_head->children_head_;
      } else { // should calculate num_of_children
        assert(file->level != 0);
        assert(cur_child != nullptr);
        cur_level = file->level;
        FileListNode* new_file_node = new FileListNode;
        new_file_node->file_ = file;
        new_file_node->next_ = nullptr;
        cur_child->next_ = new_file_node;
        cur_child = new_file_node;
        if (file->tag == kFlag) {
          l0_head->num_of_empty_children++;
        }
        l0_head->num_of_children++;
      }
    }
  }

  max_file_num_ = max_file_num;
}

FileLinkedList::~FileLinkedList() {
  auto cur = l0_head;
  while (cur) {
    auto to_del = cur;
    cur = cur->next_;
    delete to_del;
  }
}

FileLinkedList::L0_ListNode::L0_ListNode(FileMetaData* file, FileLinkedList::L0_ListNode* next) {
  children_head_ = new FileLinkedList::FileListNode;
  children_head_->file_ = file;
  children_head_->next_ = nullptr;
  num_of_children = 1;
  num_of_empty_children = 0;
  next_ = next;
}

FileLinkedList::L0_ListNode::~L0_ListNode(){
  auto cur = children_head_;
  while(cur) {
    auto to_del = cur;
    cur = cur->next_;
    delete to_del;
  }
}

// insert one node ahead of cur l0_head
void FileLinkedList::AddL0Node(FileMetaData* file) {
  if (l0_head == nullptr) {
    file->column = 0;
  } else {
    file->column = l0_head->children_head_->file_->column + 1;
  }

  l0_head = new L0_ListNode(file, l0_head);
}

bool FileLinkedList::ReplaceL0Node(FileMetaData* file, int column) {
  auto cur = l0_head;
  while(cur != nullptr) {
    if (cur->children_head_->file_->column == column) {
      // just replace file metadata
      cur->children_head_->file_ = file;
      return true;
    }
    cur = cur->next_;
  }
  return false;
}

bool FileLinkedList::ExtractOneChild(FileMetaData* file, int column) {
  auto cur = l0_head;
  while(cur != nullptr) {
    if (cur->children_head_->file_->column == column) { //find node to be replaced
      auto to_move = cur->children_head_->next_; // check if has children
      auto new_child = new FileListNode;
      new_child->file_ = file;
      cur->children_head_->next_ = new_child;
      new_child->next_ = to_move; // move origin children to new child
      cur->num_of_children++;
      while (to_move){ // level++
        if (to_move->file_) {
          to_move->file_->level++;
        }
        to_move = to_move->next_;
      }
      return true;
    }
    cur = cur->next_;
  }
  return false;
}

void FileLinkedList::MoveOtherToDeeper(std::unordered_set<uint64_t>& input_file_columns, std::vector<FileMetaData*>& file_list){
  if (input_file_columns.size() == 0) return;
  auto start = l0_head->next_;
  while (start) {
    auto cur_file = start->children_head_->file_;
    if (input_file_columns.find(cur_file->column) != input_file_columns.end()) {
      //ignore this column
      start = start->next_;
      continue;
    } else {
      MoveChildrenToDeeperLevel(start, file_list);
    }
    start = start->next_;
  }
  

}

void FileLinkedList::MoveChildrenToDeeperLevel(L0_ListNode* cur, std::vector<FileMetaData*>& file_list) {
  if(cur != nullptr) {
    assert(cur->children_head_ != nullptr);
    //insert empty nodes
    auto old_child = cur->children_head_->next_;
    auto new_child = new FileListNode;
    new_child->file_ = new FileMetaData;
    new_child->file_->tag = kFlag;
    new_child->file_->column = cur->children_head_->file_->column;
    new_child->file_->level = 1;
    new_child->next_ = old_child;
    cur->children_head_->next_ = new_child; // children_head_的next_指针没有指向new_child
    file_list.push_back(new_child->file_);
    cur->num_of_children++;
    cur->num_of_empty_children++;
    while (old_child) {
      if (old_child->file_){
        old_child->file_->level++;
      }
      old_child = old_child->next_;
    }
  }
}

bool FileLinkedList::GetVersion(int start_column, std::vector<FileMetaData*>& results) {
  if (l0_head == nullptr || start_column > l0_head->children_head_->file_->column) {
    return false;
  }
  auto start_l0_node = l0_head;
  while (start_l0_node) {
    if (start_l0_node->children_head_->file_->column == start_column) {
      break;
    }
    start_l0_node = start_l0_node->next_;
  }
  if (start_l0_node == nullptr) return false;

  results.clear();
  int width = start_l0_node->num_of_children;
  while (start_l0_node) {
    assert(start_l0_node->num_of_children >= width);
    auto cur_child = start_l0_node->children_head_;
    int w = width;
    while (w--) {
      results.push_back(cur_child->file_);
      cur_child = cur_child->next_;
    }
    start_l0_node = start_l0_node->next_;
  }

  return true;
}

// if needs merge, return start and end column
bool FileLinkedList::ShouldMerge(int& start, int& end, int merge_length) {
  auto column = l0_head->children_head_->file_->column;
  if (column > 0 && column % merge_length == 0) { // merge for every 10 columns
    start = column - 1;
    end = column - merge_length;
    return true;
  }
  return false;
}

// merge file, modify file meta data
std::vector<std::vector<FileMetaData*>> FileLinkedList::MergeColumns(int start, int end) {
  auto start_l0_node = l0_head;
  while(start_l0_node) {
    if (start_l0_node->children_head_->file_->column == start) {
      break;
    }
    start_l0_node = start_l0_node->next_;
  }
  std::vector<FileListNode*> to_merge_heads;
  int max_depth = 0;
  while(start_l0_node) {
    to_merge_heads.push_back(start_l0_node->children_head_);
    if (start_l0_node->children_head_->file_->column == end) {
      max_depth = start_l0_node->num_of_children;
      break;
    }
    start_l0_node = start_l0_node->next_;
  }

  // get file to merge
  std::vector<std::vector<FileMetaData*>> to_merge_file;
  for (int depth = 0; depth < max_depth; depth++) {
    std::vector<FileMetaData*> to_push;
    for (int i = 0; i < to_merge_heads.size(); i++) {
      if (to_merge_heads[i] != nullptr) {
        if (to_merge_heads[i]->file_->tag == kNewFile) {
          to_push.push_back(to_merge_heads[i]->file_);
        }
        if (to_merge_heads[i]->file_->tag == kMergedFile) {
          // columns from start to end have beed merged before
          // stop and do not merge
          to_merge_file.clear();
          return to_merge_file;
        }
        to_merge_heads[i] = to_merge_heads[i]->next_;
      }
    }
    if (to_push.size() > 1) {
      to_merge_file.push_back(to_push);
    }
  }
  
  return to_merge_file;
  
}

// delete useless file and meta data
void FileLinkedList::DeleteVersion(std::string dbname_, int n, std::vector<FileMetaData*>& should_delete) {
  // 1. find target node and prev node
  L0_ListNode* prev_node = nullptr;
  auto target_node = l0_head;
  while (target_node != nullptr && target_node->children_head_->file_->column != n) {
    prev_node = target_node;
    target_node = target_node->next_;
  }

  int width = (prev_node != nullptr) ? prev_node->num_of_children : 0;

  // 2. remove nodes that deeper than width and after prev
  L0_ListNode* current_node = target_node;
  L0_ListNode* prev_of_cur = prev_node;

  std::vector<FileMetaData*> to_merge;
  
  while (current_node != nullptr){
    // if level < width, save to merge
    // else should delete
    auto cur_child = current_node->children_head_;
    auto prev_of_child = cur_child;
    while (cur_child != nullptr) {
      if (cur_child->file_->level < width) {
        // save to merge
        if (cur_child->file_->tag == kNewFile) {
          to_merge.push_back(cur_child->file_);
        }
        prev_of_child = cur_child;
        cur_child = cur_child->next_;
      } else {
        //should delete
        if (cur_child->file_->tag == kNewFile) {
          should_delete.push_back(cur_child->file_);
        }
        // delete node and move to next
        prev_of_child->next_ = nullptr;
        prev_of_child = cur_child;
        auto del_node = cur_child;
        cur_child = cur_child->next_;
        delete del_node;
        current_node->num_of_children--;
      }
    }
    if (current_node->num_of_children == 0) {
      prev_of_cur->next_ = nullptr;
      prev_of_cur = current_node;
      auto del_l0_node = current_node;
      current_node = current_node->next_;
      delete del_l0_node;
    } else {
      prev_of_cur = current_node;
      current_node = current_node->next_;
    }
  }
  
  if (prev_node == nullptr) { 
    // all nodes are deleted, reset l0_head
    l0_head = nullptr;
  }

  // TODO:
  // merge files to corresponding level
  

  
}


bool FileLinkedList::GetOverlappedFilesL0(std::vector<FileMetaData*>& results){
  if (l0_head == nullptr) return false;
  auto head_smallest = l0_head->children_head_->file_->smallest;
  auto head_largest = l0_head->children_head_->file_->largest;

  auto cur = l0_head->next_;
  while (cur) {
    if (cur->children_head_->file_->tag == kFlag) {
      cur = cur->next_;
      continue;
    }
    auto cur_smallest = cur->children_head_->file_->smallest;
    auto cur_largest = cur->children_head_->file_->largest;

    if (cur_smallest > head_largest || cur_largest < head_smallest) {
      cur = cur->next_; // no overlapping
      continue;
    }
    results.push_back(cur->children_head_->file_);
    cur = cur->next_;
  }

  return true;

}

FileMetaData* FileLinkedList::getHeadFileMeta() {
  if (l0_head) return l0_head->children_head_->file_;
}

uint64_t FileLinkedList::NextFileNumber() {
  max_file_num_++;
  return max_file_num_;
}


void FileLinkedList::PrintList() {
  auto start_l0_node = l0_head;
  while (start_l0_node) {
    auto cur_child = start_l0_node->children_head_;
    while (cur_child) {
      std::cout << cur_child->file_->tag << "\t";
      cur_child = cur_child->next_;
    }
    std::cout << std::endl;
    start_l0_node = start_l0_node->next_;
  }
}


}
