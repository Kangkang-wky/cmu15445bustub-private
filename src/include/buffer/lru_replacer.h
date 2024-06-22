//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

// 双向链表 + 哈希表的方式实现 LRU 方式

class LinkListKV {
 public:
  frame_id_t val_{0};
  LinkListKV *next_{nullptr};
  LinkListKV *pre_{nullptr};
  explicit LinkListKV(frame_id_t val) : val_(val) {}
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  // 哈希表 : (key : value) value 是双向链表的节点
  std::unordered_map<frame_id_t, LinkListKV *> hashmap_frame_;
  LinkListKV *head_;
  LinkListKV *tail_;
  std::mutex latch_;

  size_t capacity_{0};
  size_t size_{0};

  void RemoveNode(LinkListKV *frameNode);
  void MovetoTail(LinkListKV *frameNode);
  void InserttoTail(LinkListKV *frameNode);
};

}  // namespace bustub
