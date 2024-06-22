//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * 实例化的时候, 需要关注 两个主要, 一是替换策略, 二是 buffer 池的大小
   * @brief Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the lookback constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t override { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

 protected:
  /**
   * TODO(P1): Add implementation
   * 函数作用: 在 BPM 中创建一个新的 page, 将 page_id 设置为新页面的 id
   * 如果 所有 frame 现在都被使用, 并且是无法驱逐的, 返回一个 nullptr 即可
   *
   * 检查是否有 free_list 可用, 如果没有那么 replacer.evict 驱逐一个
   * frame,(若该frame的is_dirty_为true，那么需要先将frame中的内容写回对应的物理页)
   *
   * 通过 AllocatorPage() 分配一个 page_id, 然后 记录 frame 对应的 page_id, 并将 pin_count 设置为 1
   * 随后通过 RecordAccess 记录 访问历史, 同时将 frame 设置为不可驱逐的  setevictable
   *
   * 如果 frame 上有脏位, 应该先将其写回磁盘, 还需要重置页面的内存和元数据
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPgImp(page_id_t *page_id) -> Page * override;

  /**
   * TODO(P1): Add implementation
   *
   * 先通过 frame_id 找到对应的 frame, 如果找得到, 那么只要增加 pin_count, 设置 evictable 为 false 并且 RecordAccess
   * 如果找不到, 需要像上面一样分配一个 new page, 并且重新设置 page_table, 将磁盘中的 page 读出来,
   * 然后仍然需要 RecordAccess 并且 SetUnevictable.
   *
   * 函数目的: 从 buffer pool 中选取一个 page, 如果 page_id 需要从 disk 上读取, 并且目前的 pool 里面都在被使用和无法驱逐
   * 在实现 NewPgImp 之后再实现
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
   * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
   * to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
   *
   * @param page_id id of page to be fetched
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  auto FetchPgImp(page_id_t page_id) -> Page * override;

  /**
   * TODO(P1): Add implementation
   *
   * 在对应的 frame_id 存在 并且 pin_count 不为 0 的时候才可以 unpin, 只需要 --pin_count
   * 如果为 0，则表明进程都是 unuse 的状态, 调用 LRU 替换机制中的 setevictable 将其设置为 true
   * 注意，在这个过程中要正确设置 page 元数据 is_dirty.
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;

  /**
   * TODO(P1): Add implementation
   *
   * FlushPage: 将给定给的缓存页 Page 写回磁盘, 并且设置 is_dirty 为 false(表明无视 page is dirty 的标志)
   * 通过 page_id 找到 frame_id 然后 磁盘 写回 清空 is_dirty
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPgImp(page_id_t page_id) -> bool override;

  /**
   * TODO(P1): Add implementation
   *
   * FlushPage: 写回所有的 Page 并且设置 is_dirty 为 false.(同上)
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;

  /**
   * TODO(P1): Add implementation
   *
   * DeletePage 则是先要写回（在 pin_count 为 0 的前提下), 然后再驱逐 Page, 并且归还给磁盘 (DeallocatePage),
   * 从 LRU 中移除这个 frame 并且从 page_table 等删掉, 然后把 frame_id 重新加入 free_list.
   *
   * 从 buffer pool 中删除 page 如果 page id 不在内存池中, 啥也不做 return true
   * 如果 page 是 pin 的 并且不能删除返回 false
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePgImp(page_id_t page_id) -> bool override;

  // buffer pool LRU-K buffer 池的大小
  // bucket_size 桶大小
  // page_id 维护原子属性
  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;
  /** Bucket size for the extendible hash table */
  const size_t bucket_size_ = 4;

  /** Array of buffer pool pages. */
  // buffer pool 上的 pages
  Page *pages_;

  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));

  /** Page table for keeping track of buffer pool pages. */
  // 通过可扩展哈希维护 BPM 中 page_id 与 frame_id 的关系
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;

  /** Replacer to find unpinned pages for replacement. */
  // 之前写的 LRU 替换策略
  LRUKReplacer *replacer_;

  // list 上维护好 frame_id, 这些 frame 上还没有 page
  /** List of free frames that don't have any pages on them. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;

  /**
   * 在磁盘上分配一个 allocate page
   *
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  // TODO(student): You may add additional private members and helper functions
};
}  // namespace bustub
