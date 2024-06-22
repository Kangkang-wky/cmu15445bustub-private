//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <queue>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 *
 * 经典的缓存驱逐策略有：
 * FIFO 先进先出驱逐 
 * LRU  最近最久未被访问驱逐
 * LRU_K : 按照 k-distance 定义, 每次驱逐 k-distance 最大的 frame
 * 考虑以下两种情况
 * (1) 对于不超过 k 次的访问按照 k-distance = +inf 来计算, 如果有多个 +inf 的 case, 按照先进先出驱逐,
 * 即驱逐最早的 frame
 * (2) 对于超过 k 次的访问按照 k-distance = 倒数第 k 次出现的位置来计算, 内部更像一个 LRU 驱逐的形式
 * 驱逐最近最久未被访问的
 * implementation: 需要维护两个队列, 一个是 History list, 一个是 Cache list
 * 可以使用 std::list 实现一个低性能版本 ： <frame, std::list迭代器>    <frame, std::list迭代器>
 * 再使用自己构建的数据结构 LinkedListNode, 实现一个高性能版本
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   * num_frames 是 LRU 的总数, k 是记录的次数
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   * Evict : 注意事项:
   * (1) 驱逐要满足 k-distance 中最大的一个
   * (2) 同时要求这个 frame 是可驱逐的, 即 evictable 的, 驱逐规则按照上面给定的即可
   * (3) 通过 frame_id 返回需要驱逐的表项
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   * 在当前时间戳下 记录 frame id 作为一个 frame_id + 时间戳的 event 事件
   *
   * 如果 frame id 是非法的, 抛出一个 exception 即可, 也可以使用 BUSTUB_ASSERT 终止进程
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   * 此处设置 frame_id 是可驱逐还是不可驱逐的, 同时, 注意 frame_id 由可驱逐变为不可驱逐, LRU replacer
   * size 会减少, 相当于可以操作的 frame 减少了
   *
   * 注意 replacer size = 可驱逐的 entry 的数量
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *  从 replacer 中移除 frame, 包括它的访问历史, 即将其访问次数置 0, 并将表项删除
   *  如果移除成功, size --
   *  - 该函数移除特定的 frame_id
   *  - 如果移除一个 non-evictable 的 frame, 抛异常 或者 终止进程即可
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * 返回可驱逐的 frame 的数量, 即 curr_size_ 大小更新返回
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0};  // 时间戳
  size_t curr_size_{0};          // 返回目前 LRU' Replacer 实际大小
  size_t replacer_size_;         // LRU' Replacer 支持的最大 frame 数量
  size_t k_;                     // LRU-K
  std::mutex latch_;             // 锁

  // hashmap + list 组合, list 按照头插法, 新的 frame 插在 list 头部
  std::list<frame_id_t> history_list_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> history_hashmap_;
  std::list<frame_id_t> cache_list_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> cache_hashmap_;

  // 记录 frame_id 的基础属性
  using timestamp = std::list<size_t>;
  std::unordered_map<frame_id_t, bool> is_evictable_;         // 是否可驱逐
  std::unordered_map<frame_id_t, size_t> record_cnt_;         // 被访问次数
  std::unordered_map<frame_id_t, timestamp> timestamp_list_;  // frame 的访问时间戳队列
};

}  // namespace bustub
