//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  // pages_ 即 frame id 的空间
  // page_table_ 即 page_id, frame_id
  // replacer 即 LEU 替换策略
  // free_list_ 事先实现 free list 即可
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // free list 非空 可以分配
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  // free list 为空, 需要考虑进行驱逐策略, 若驱逐成功
  else if (replacer_->Evict(&frame_id)) {
    page_id_t page_evict = pages_[frame_id].GetPageId();

    // 如果驱逐的 page 是 is_dirty 的
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(page_evict, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }

    // 设置 pages 属性
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;

    // page_table_ 可扩展哈希移除 page_evict 部分
    page_table_->Remove(page_evict);

  }
  // 上述两个条件皆不满足
  else {
    return nullptr;
  }

  // 分配 page 页面
  *page_id = AllocatePage();

  // buffer pool 属性设置
  // "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  // LRU-K 设置
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // page 属性设置
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // 在 page_table 中能够找到
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }

  // 在 page_table 中找不到, 从 disk 上读取, 做法与 new 类似

  // free list 非空 可以分配
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  // free list 为空, 需要考虑进行驱逐策略, 若驱逐成功
  else if (replacer_->Evict(&frame_id)) {
    page_id_t page_evict = pages_[frame_id].GetPageId();

    // 如果驱逐的 page 是 is_dirty 的
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(page_evict, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }

    // 设置 pages 属性
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;

    // page_table_ 可扩展哈希移除 page_evict 记录
    page_table_->Remove(page_evict);
  }
  // 上述两个条件皆不满足
  else {
    return nullptr;
  }

  // 分配 page 页面

  // buffer pool 属性设置
  // "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  // LRU-K 设置
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // page 属性设置
  pages_[frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // not found
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  // unpin
  pages_[frame_id].pin_count_--;

  // is dirty
  pages_[frame_id].is_dirty_ = is_dirty ? is_dirty : !is_dirty;

  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // 如果 page_id 是非法的 page id
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (int i = 0; i < this->pool_size_; i++) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // can found & pin count == 0
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);

  // replacer & free_list & page_table
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  page_table_->Remove(page_id);

  // page init
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
