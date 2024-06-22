//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (Size() == 0) {
    return false;
  }

  // 按照驱逐策略正常做, 先驱逐 history, 再驱逐 cache
  for (auto history_iter = history_list_.rbegin(); history_iter != history_list_.rend(); history_iter++) {
    auto frame_id_temp = *history_iter;

    if (is_evictable_[frame_id_temp]) {
      history_list_.erase(history_hashmap_[frame_id_temp]);  // return 结束了, 迭代器增加也是安全的
      history_hashmap_.erase(frame_id_temp);
      curr_size_--;
      *frame_id = frame_id_temp;

      // 相关表项清除 frame_id_temp 的记录
      record_cnt_.erase(frame_id_temp);
      // 暂时保留是否可驱逐的性质
      // is_evictable_.erase(frame_id_temp);
      timestamp_list_.erase(frame_id_temp);
      return true;
    }
  }

  for (auto cache_iter = cache_list_.rbegin(); cache_iter != cache_list_.rend(); cache_iter++) {
    auto frame_id_temp = *cache_iter;

    if (is_evictable_[frame_id_temp]) {
      cache_list_.erase(cache_hashmap_[frame_id_temp]);
      cache_hashmap_.erase(frame_id_temp);
      curr_size_--;
      *frame_id = frame_id_temp;

      // 相关表项清除 frame_id_temp 的记录
      record_cnt_.erase(frame_id_temp);
      // 暂时保留是否可驱逐的性质
      // is_evictable_.erase(frame_id_temp);
      timestamp_list_.erase(frame_id_temp);
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // 细粒度锁控制
  latch_.lock();

  // frame_id 异常
  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    throw std::exception();
  }

  // 这里默认 k > 1 进行讨论, 测试用例中没有 k == 1 的讨论

  // 判断 frame_id 是否存在, 不存在添加

  // 以前没出现过
  if (record_cnt_.count(frame_id) == 0U) {
    if (Size() == replacer_size_) {
      // 这个时候已经满了, 需要进行驱逐策略
      frame_id_t evict_frame_id;
      latch_.unlock();
      bool evict_flag = Evict(&evict_frame_id);
      latch_.lock();
      // 驱逐失败, 直接退出, 因为满的情况下无法访问
      if (!evict_flag) {
        latch_.unlock();
        return;
      }
    }
    // 驱逐成功或者 Size() 已经小于 replace_size 的时候

    current_timestamp_++;
    record_cnt_[frame_id]++;
    is_evictable_[frame_id] = false;  // 默认是 false
    // 因为默认设置为 false, 所以不设置 cur_size_++ 操作
    timestamp_list_[frame_id].emplace_front(current_timestamp_);

    // 因为没出现过, if 必然为 true
    if (history_hashmap_.count(frame_id) == 0) {
      history_list_.push_front(frame_id);
      history_hashmap_[frame_id] = history_list_.begin();
    }
  }
  // 以前出现过, 这种情况 curr_size_ 保持原来得大小
  else {
    current_timestamp_++;
    record_cnt_[frame_id]++;
    // is_evictable_[frame_id] = false; 以前出现过 is_evictable 表不更新
    timestamp_list_[frame_id].emplace_front(current_timestamp_);

    // 访问第 k 次了
    if (record_cnt_[frame_id] == k_) {
      // 从 history list 中删除
      history_list_.erase(history_hashmap_[frame_id]);
      history_hashmap_.erase(frame_id);

      // 从 cache list 中添加
      cache_list_.push_front(frame_id);
      cache_hashmap_[frame_id] = cache_list_.begin();
    }
    // 访问超过了 k 次
    else if (record_cnt_[frame_id] > k_) {
      if (cache_hashmap_.count(frame_id) != 0) {
        cache_list_.erase(cache_hashmap_[frame_id]);
      }
      cache_list_.push_front(frame_id);
      cache_hashmap_[frame_id] = cache_list_.begin();
    }
    // 访问少于 k 次可以不用管, 因为 history 队列是 FIFO 的
    // 不用更新 history list 的顺序
  }

  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  // If frame id is invalid, throw an exception or abort the process.
  // 如何检查非法的 frame_id

  // 这里保证 frame_id 是一定存在哈希表中的
  if (record_cnt_.count(frame_id) == 0) {
    return;
  }

  bool pre_evictable = is_evictable_[frame_id];
  is_evictable_[frame_id] = set_evictable;

  // 之前不 evict 现在可 evict
  if (!pre_evictable && set_evictable) {
    curr_size_++;
  }
  // 之前可 evict 现在不可 evict
  else if (pre_evictable && !set_evictable) {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  // 移除特定 frame_id 的 frame page
  // 判断对应 frame_id 是否存在表项中
  if (record_cnt_.count(frame_id) == 0) {
    return;
  }

  // 移除一个 non-evictable frame, 选择直接抛异常
  if (!is_evictable_[frame_id]) {
    throw std::exception();
  }

  // 如果 record_cnt_ < k_
  if (record_cnt_[frame_id] < k_) {
    history_list_.erase(history_hashmap_[frame_id]);
    history_hashmap_.erase(frame_id);
  }
  // 如果 record_cnt_ >= k
  else {
    cache_list_.erase(cache_hashmap_[frame_id]);
    cache_hashmap_.erase(frame_id);
  }

  --curr_size_;

  is_evictable_.erase(frame_id);
  record_cnt_.erase(frame_id);
  timestamp_list_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
