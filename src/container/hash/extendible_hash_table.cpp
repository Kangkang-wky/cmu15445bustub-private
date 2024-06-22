//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // 可扩展哈希表初始化: 首先初始化 Bucket, 将 Bucket 内的 local depth 置 0
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

// 以下三个 getGlobalDepth, GetLocalDepth, GetNumBuckets 采用锁结构保护一下

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  // 使用 RAII 锁 完成粗粒度的并发控制
  std::scoped_lock<std::mutex> lock(latch_);

  auto bucket = dir_[IndexOf(key)];

  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  // 根据 note 提示, 并不做 hash 表的 Shrink & Combination 操作
  std::scoped_lock<std::mutex> lock(latch_);

  auto bucket = dir_[IndexOf(key)];

  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  // 注意此处必须修改为循环插入的形式, 因为桶分裂重新分配 key-value 对的时候
  // 将导致
  while (true) {
    auto bucket = dir_[IndexOf(key)];

    // 如果 bucket->Insert(key, value) false 表明桶是满的
    if (bucket->Insert(key, value)) {
      break;
    }

    // 不能插入, 桶是满的情况下

    // 如果 global depth == local depth 目录需要翻倍
    if (bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int old_dir_size = dir_.size();
      dir_.resize(old_dir_size << 1);
      for (int i = old_dir_size; i < old_dir_size << 1; i++) {
        dir_[i] = dir_[i - old_dir_size];
      }
    }

    int mask = 1 << bucket->GetDepth();
    bucket->IncrementDepth();
    num_buckets_++;

    // auto bucket_0 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());

    for (auto it = bucket->GetItems().begin(); it != bucket->GetItems().end();) {
      size_t hash_key = std::hash<K>()(it->first);
      if ((hash_key & mask) != 0U) {
        new_bucket->Insert(it->first, it->second);
        // 此处注意迭代器 it 的更新
        bucket->GetItems().erase(it++);
      } else {
        it++;
      }
    }

    // 更新 dir_ 目录重新映射 entry pointer 与 bucket
    for (size_t entry_index = 0; entry_index < dir_.size(); entry_index++) {
      if (dir_[entry_index] == bucket && (entry_index & mask) != 0U) {
        dir_[entry_index] = new_bucket;
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  // using iterator = typename std::list<std::pair<K, V>>::iterator; 迭代器
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      // 别写反了, 需求是找值, 注意返回桶内的 value
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  // std::any_of + lambda function body
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    if (item.first == key) {
      this->list_.remove(item);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  // 如果是在 bucket 中存在
  for (auto &[list_key, list_value] : list_) {
    if (list_key == key) {
      // 修改 list_ 中 value 的值
      list_value = value;
      return true;
    }
  }

  // 如果桶是满的话, do nothing 直接返回
  if (IsFull()) {
    return false;
  }

  // 其他情况, emplace_back <key, value>对
  list_.emplace_back(std::make_pair(key, value));

  return true;
}

// 下面是一些模板特化的部分
template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
