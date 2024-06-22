//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   * 构造函数初始化, 给出 bucket_size 大小
   * 将 global_depth 置 0 num_bucket 置 1
   * 往 dir_ 中增加条目 entry
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   *
   * dir 中 entry 数目的多少即 global depth
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * 对应某一个 entry 的 bucket 的当前的 Bucket 的 local depth 是多少
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   *
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   *
   * TODO(P1): Add implementation
   * 通过给定的 key 来寻找 value, 通过 key 定位到 哪个 entry
   * 再通过 entry 定位到 bucket, bucket 内查询 key find(key, value)
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override;

  /**
   *
   * TODO(P1): Add implementation
   * 对于哈希表的插入操作做如下讨论：
   * (1) key 存在, 直接更新桶中的 value 值
   * (2) key 不存在
   *     （i） 如果 bucket 不是满的, 直接在 bucket 中插入 key-value 即可
   *      (ii) 如果 bucket 是满的, (插入失败)需要继续讨论
   *           (-)  如果桶的 local depth == 目录的 global depth
   *                第一：global depth++
   *                第二：将 dir entry 进行 double, dir entry 先映射旧有的 bucket
   *           (--) 如果桶的 local depth < 目录的 global depth
   *
   *           final:
   *                第一：bucket 的 local depth++
   *                第二：num_buckets++
   *                第三：构建 new_bucket, 将旧桶中的 key_value 对映射到 bucket 与 new_bucket
   *                      凭借 local mask 的构造, 映射 key_value 到两个 bucket
   *                第四：将 dir entry 目录中指向 old_bucket 的, 根据 local mask 重新映射到
   *                      new_bucket
   * 重新尝试插入
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override;

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    // 简单的构造函数初始化
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full.  检查桶是不是满的 */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. 返回桶的 local depth*/
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. 在桶分裂的时候对桶++ */
    inline void IncrementDepth() { depth_++; }

    /** @brief Return Bucket KV list 返回桶中存储的KV 序列对 */
    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     *
     * TODO(P1): Add implementation
     * Bucket 类查找对应的 key 和 value 是否在 bucket 桶内
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     * 移除 对应 key 的 bucket 桶内的 key-value 对
     * 这里只是哈希表移除 key-value 对, 并不做所谓的 shrink 操作
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     * 注意此处逻辑, 如果 key 已经存在, 更新 value 值
     * 再判断 bucket 是否是满的, 以此来判断
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool;

   private:
    // TODO(student): You may add additional private members and helper functions
    // 官方实现的 Bucket 是一个 list_
    // size_ 指的是 Bucket size 大小
    // depth_ 指的是 local depth 大小
    // list_ 指的是 Bucket 中实际存储的键值对
    size_t size_;
    int depth_;
    std::list<std::pair<K, V>> list_;
  };

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.
  // 成员变量的实现
  // global_depth_ = n
  // bucket_size_ = bucket 内部的大小
  // num_buckets 是 buckets 的数量
  // dir 线性表存储指向 Bucket 的指针
  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket, int64_t index) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * 返回 key 经过 hash 函数后的低 global_depth_ 位
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
