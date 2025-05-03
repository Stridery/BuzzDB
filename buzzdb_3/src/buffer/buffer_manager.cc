#include <cassert>
#include <iostream>
#include <string>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include <chrono>
#include <ctime> 
#include <condition_variable>
#include <unordered_map>
#include <list>
#include <set>
#include <queue>

uint64_t wake_timeout_ = 100;
uint64_t timeout_ = 2;

namespace buzzdb {

char* BufferFrame::get_data() { return data.data(); }


BufferFrame::BufferFrame()
    : page_id(INVALID_PAGE_ID),
      frame_id(INVALID_FRAME_ID),
      dirty(false),
      exclusive(false) {}

BufferFrame::BufferFrame(const BufferFrame& other)
    : page_id(other.page_id),
      frame_id(other.frame_id),
      data(other.data),
      dirty(other.dirty),
      exclusive(other.exclusive) {}

BufferFrame& BufferFrame::operator=(BufferFrame other) {
  std::swap(this->page_id, other.page_id);
  std::swap(this->frame_id, other.frame_id);
  std::swap(this->data, other.data);
  std::swap(this->dirty, other.dirty);
  std::swap(this->exclusive, other.exclusive);
  return *this;
}



BufferManager::BufferManager(size_t page_size, size_t page_count) {
  capacity_ = page_count;
  page_size_ = page_size;

  pool_.resize(capacity_);
  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    pool_[frame_id].reset(new BufferFrame());
    pool_[frame_id]->data.resize(page_size_);
    pool_[frame_id]->frame_id = frame_id;
  }
}

BufferManager::~BufferManager() {
}



/*
BufferFrame& BufferManager::fix_page(UNUSED_ATTRIBUTE uint64_t txn_id, uint64_t page_id, bool exclusive) {
  //std::cout << " fix_page" << std::endl;
  
  LockMode mode = exclusive ? LockMode::EXCLUSIVE : LockMode::SHARED;

  auto& lock = lock_manager.lock_table[page_id];
  std::unique_lock<std::mutex> lk(lock.mutex);

  if (lock.exclusive_holder == txn_id) {
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (pool_[frame_id]->page_id == page_id) {
        pool_[frame_id]->exclusive = exclusive;
        return *pool_[frame_id];
      }
    }
  }

  if (!exclusive && lock.shared_holders.count(txn_id)) {
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (pool_[frame_id]->page_id == page_id) {
        pool_[frame_id]->exclusive = false;
        return *pool_[frame_id];
      }
    }
  }

  lock.queue.emplace_back(txn_id, mode);
  auto it = --lock.queue.end();

  auto can_grant = [&]() {
    if (it->mode == LockMode::SHARED) {
      for (auto& r : lock.queue) {
        if (&r == &(*it)) break;
        if (r.mode == LockMode::EXCLUSIVE) return false;
      }
      return lock.exclusive_holder == 0;
    } else {
      return lock.shared_holders.empty() && lock.exclusive_holder == 0;
    }
  };

  while (!can_grant()) {
    lock_manager.waits_for[txn_id].clear();
    if (mode == LockMode::SHARED) {
      for (auto& r : lock.queue) {
        if (&r == &(*it)) break;
        if (r.mode == LockMode::EXCLUSIVE && r.granted) {
          lock_manager.waits_for[txn_id].insert(r.txn_id);
        }
      }
      if (lock.exclusive_holder != 0)
        lock_manager.waits_for[txn_id].insert(lock.exclusive_holder);
    } else {
      // exclusive mode
      for (auto& r : lock.queue) {
        if (&r == &(*it)) break;
        if (r.granted) {
          lock_manager.waits_for[txn_id].insert(r.txn_id);
        }
      }
      for (auto holder : lock.shared_holders) {
        if (holder != txn_id)
          lock_manager.waits_for[txn_id].insert(holder);
      }
      if (lock.exclusive_holder != 0 && lock.exclusive_holder != txn_id)
        lock_manager.waits_for[txn_id].insert(lock.exclusive_holder);
    }
    std::set<uint64_t> visited;
    if (lock_manager.detect_deadlock(txn_id, visited)) {
      //lock.queue.erase(it);
      //lock_manager.waits_for.erase(txn_id);
      transaction_abort(txn_id);
      throw transaction_abort_error();
    }
    it->cv.wait_for(lk, std::chrono::milliseconds(wake_timeout_));
  }

  it->granted = true;
  if (mode == LockMode::SHARED) {
    lock.shared_holders.insert(txn_id);
  } else {
    lock.exclusive_holder = txn_id;
  }
  lock_manager.txn_to_pages[txn_id].insert(page_id);
  lock_manager.waits_for.erase(txn_id);

  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->page_id == page_id) {
			pool_[frame_id]->exclusive = exclusive;
			return *pool_[frame_id];
		}
	}

  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->page_id == INVALID_PAGE_ID) {
			pool_[frame_id]->page_id = page_id;
      pool_[frame_id]->exclusive = exclusive;
      pool_[frame_id]->dirty = false;
      read_frame(pool_[frame_id]->frame_id);
			return *pool_[frame_id];
		}
	}

  for (auto it = lock.queue.begin(); it != lock.queue.end(); ) {
    if (it->txn_id == txn_id && it->granted) {
      it = lock.queue.erase(it);
    } else {
      ++it;
    }
  }
  lock.shared_holders.erase(txn_id);
  if (lock.exclusive_holder == txn_id) {
    lock.exclusive_holder = 0;
  }
  lock_manager.txn_to_pages[txn_id].erase(page_id);
  for (auto& req : lock.queue) {
    req.cv.notify_all();
  }

  throw buffer_full_error();
}
*/

BufferFrame& BufferManager::fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive) {
  LockMode mode = exclusive ? LockMode::EXCLUSIVE : LockMode::SHARED;

  auto& lock = lock_manager.lock_table[page_id];
  std::unique_lock<std::mutex> lk(lock.mutex);

  std::cout << txn_id << " fix page " << exclusive << std::endl;
  std::cout << "share holders: ";
  for(auto s_it = lock.shared_holders.begin(); s_it != lock.shared_holders.end(); s_it++){
    std::cout << *s_it << " ";
  }
  std::cout << std::endl;
  std::cout << "exclusive holder: " << lock.exclusive_holder << std::endl;

  // Case 1: already holds EXCLUSIVE
  if (lock.exclusive_holder == txn_id) {
    std::cout << txn_id <<  "case 1" << std::endl;
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (pool_[frame_id]->page_id == page_id) {
        pool_[frame_id]->exclusive = exclusive;
        return *pool_[frame_id];
      }
    }
  }

  // Case 2: already holds SHARED and requests SHARED again
  if (!exclusive && lock.shared_holders.count(txn_id)) {
    std::cout << txn_id <<  " case 2" << std::endl;
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (pool_[frame_id]->page_id == page_id) {
        pool_[frame_id]->exclusive = false;
        return *pool_[frame_id];
      }
    }
  }

  // Case 3: Lock upgrade SHARED -> EXCLUSIVE
  if (exclusive && lock.shared_holders.count(txn_id)) {
    std::cout << txn_id <<  " case 3" << std::endl;
    if (lock.upgrading_txn_id == 0 || lock.upgrading_txn_id == txn_id) {
      lock.upgrading_txn_id = txn_id;
      lock.queue.emplace_back(txn_id, LockMode::EXCLUSIVE);
      auto it = --lock.queue.end();

      auto can_upgrade = [&]() {
        return (lock.shared_holders.size() == 1 &&
                lock.shared_holders.count(txn_id) &&
                lock.exclusive_holder == 0 &&
                lock.upgrading_txn_id == txn_id &&
                &(*it) == &lock.queue.front());
      };

      auto start_time = std::chrono::steady_clock::now();
      std::cout << txn_id << " can upgrade? " << can_upgrade() << std::endl;
      std::cout << txn_id << " share holder size: " << lock.shared_holders.size() << std::endl;
      while (!can_upgrade()) {
        std::cout << txn_id <<  " waiting" << std::endl;
        lock_manager.waits_for[txn_id] = lock.shared_holders;
        lock_manager.waits_for[txn_id].erase(txn_id);
        if (lock.exclusive_holder != 0)
          lock_manager.waits_for[txn_id].insert(lock.exclusive_holder);

        std::set<uint64_t> visited;
        if (lock_manager.detect_deadlock(txn_id, visited)) {
          lock.upgrading_txn_id = 0;
          lock.queue.erase(it);
          std::cout << txn_id <<  " dead lock abort" << std::endl;
          transaction_abort(txn_id);
          throw transaction_abort_error();
        }

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
        if (static_cast<uint64_t>(elapsed) > timeout_ * 1000) {
          //lock.queue.erase(it);
          std::cout << txn_id <<  " waiting abort" << std::endl;
          lock.upgrading_txn_id = 0;
          transaction_abort(txn_id);
          throw transaction_abort_error();
        }

        it->cv.wait_for(lk, std::chrono::milliseconds(wake_timeout_));
        std::cout << txn_id <<  " wake" << std::endl;
      }


      std::cout << txn_id << " granting" << std::endl;
      it->granted = true;
      //lock.shared_holders.erase(txn_id);
      lock.exclusive_holder = txn_id;
      //lock.queue.erase(it);
      lock.upgrading_txn_id = 0;

      lock_manager.txn_to_pages[txn_id].insert(page_id);
      lock_manager.waits_for.erase(txn_id);

      for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        if (pool_[frame_id]->page_id == page_id) {
          pool_[frame_id]->exclusive = true;
          return *pool_[frame_id];
        }
      }
    }
    else{
      transaction_abort(txn_id);
      throw transaction_abort_error();
    }
  }

  // Case 4: New lock request
  std::cout << txn_id <<  "case 4" << std::endl;
  lock.queue.emplace_back(txn_id, mode);
  auto it = --lock.queue.end();

  auto can_grant = [&]() {
    if (it->mode == LockMode::SHARED) {
      for (auto& r : lock.queue) {
        if (&r == &(*it)) break;
        if (r.mode == LockMode::EXCLUSIVE) return false;
      }
      return lock.exclusive_holder == 0 && lock.upgrading_txn_id == 0;
    } else {
      return lock.shared_holders.empty() && lock.exclusive_holder == 0 && lock.upgrading_txn_id == 0 && &(*it) == &lock.queue.front();
    }
  };

  auto start_time = std::chrono::steady_clock::now();

  while (!can_grant()) {
    std::cout << txn_id << " case 4 cant grant" << std::endl;
    lock_manager.waits_for[txn_id].clear();

    if (mode == LockMode::SHARED) {
      for (auto& r : lock.queue) {
        if (&r == &(*it)) break;
        if (r.mode == LockMode::EXCLUSIVE && r.granted)
          lock_manager.waits_for[txn_id].insert(r.txn_id);
      }
      if (lock.exclusive_holder != 0)
        lock_manager.waits_for[txn_id].insert(lock.exclusive_holder);
      if (lock.upgrading_txn_id != 0)
        lock_manager.waits_for[txn_id].insert(lock.upgrading_txn_id);
    } else {
      for (auto& r : lock.queue) {
        if (&r == &(*it)) break;
        if (r.granted)
          lock_manager.waits_for[txn_id].insert(r.txn_id);
      }
      for (auto holder : lock.shared_holders) {
        if (holder != txn_id)
          lock_manager.waits_for[txn_id].insert(holder);
      }
      if (lock.exclusive_holder != 0 && lock.exclusive_holder != txn_id)
        lock_manager.waits_for[txn_id].insert(lock.exclusive_holder);
      if (lock.upgrading_txn_id != 0 && lock.upgrading_txn_id != txn_id)
        lock_manager.waits_for[txn_id].insert(lock.upgrading_txn_id);
    }

    std::set<uint64_t> visited;
    if (lock_manager.detect_deadlock(txn_id, visited)) {
      //lock.queue.erase(it);
      std::cout << txn_id << " case 4 deadlock abort" << std::endl;
      transaction_abort(txn_id);
      throw transaction_abort_error();
    }

    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
    if (static_cast<uint64_t>(elapsed) > timeout_ * 1000) {
      std::cerr << txn_id << " case 4 lock wait timed out\n";
      //lock.queue.erase(it);
      transaction_abort(txn_id);
      throw transaction_abort_error();
    }

    it->cv.wait_for(lk, std::chrono::milliseconds(wake_timeout_));
  }
  std::cout << txn_id << " case 4 granting" << std::endl;
  it->granted = true;
  if (mode == LockMode::SHARED) {
    std::cout << txn_id << " insert share holder" << std::endl;
    lock.shared_holders.insert(txn_id);
  } else {
    std::cout << txn_id << " set exclusive holder" << std::endl;
    lock.exclusive_holder = txn_id;
  }

  //lock.queue.erase(it);
  lock_manager.txn_to_pages[txn_id].insert(page_id);
  lock_manager.waits_for.erase(txn_id);

  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    if (pool_[frame_id]->page_id == page_id) {
      pool_[frame_id]->exclusive = exclusive;
      return *pool_[frame_id];
    }
  }

  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    if (pool_[frame_id]->page_id == INVALID_PAGE_ID) {
      pool_[frame_id]->page_id = page_id;
      pool_[frame_id]->exclusive = exclusive;
      pool_[frame_id]->dirty = false;
      read_frame(pool_[frame_id]->frame_id);
      return *pool_[frame_id];
    }
  }

  throw buffer_full_error();
  
}

void BufferManager::unfix_page(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE BufferFrame& page, UNUSED_ATTRIBUTE bool is_dirty) {

  
  std::cout << txn_id <<  " unfix_page" << std::endl;
  
  if (!page.dirty) {
    //write_frame(page.frame_id);
    page.dirty = is_dirty;
  }
  page.exclusive = false;

  uint64_t page_id = page.page_id;
  auto& lock = lock_manager.lock_table[page_id];
  std::unique_lock<std::mutex> lk(lock.mutex);
  for (auto it = lock.queue.begin(); it != lock.queue.end(); ) {
    if (it->txn_id == txn_id && it->granted) {
      it = lock.queue.erase(it);
    } else {
      ++it;
    }
  }
  lock.shared_holders.erase(txn_id);
  if (lock.exclusive_holder == txn_id) {
    lock.exclusive_holder = 0;
  }
  lock_manager.txn_to_pages[txn_id].erase(page_id);
  for (auto& req : lock.queue) {
    req.cv.notify_all();
  }
  
  
  
  
  
}

void  BufferManager::flush_all_pages(){
  
  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    if (pool_[frame_id]->dirty) {
      write_frame(frame_id);
      pool_[frame_id]->dirty = false;
    }
  }
  
  
  
}


void  BufferManager::discard_all_pages(){
 for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    pool_[frame_id].reset(new BufferFrame());
    pool_[frame_id]->page_id = INVALID_PAGE_ID;
    pool_[frame_id]->frame_id = frame_id;
    pool_[frame_id]->dirty = false;
    pool_[frame_id]->data.resize(page_size_);
    pool_[frame_id]->exclusive = false;
  }

  auto it = lock_manager.txn_to_pages.begin();
  while(it != lock_manager.txn_to_pages.end()){
    it->second.clear();
    it++;
  }
}

void BufferManager::discard_pages(UNUSED_ATTRIBUTE uint64_t txn_id){
  // Discard all pages acquired by the transaction 
  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    if (lock_manager.txn_to_pages[txn_id].count(pool_[frame_id]->page_id)) {
      pool_[frame_id].reset(new BufferFrame());
      pool_[frame_id]->page_id = INVALID_PAGE_ID;
      pool_[frame_id]->dirty = false;
      pool_[frame_id]->frame_id = frame_id;
      pool_[frame_id]->data.resize(page_size_);
      pool_[frame_id]->exclusive = false;

      lock_manager.txn_to_pages[txn_id].erase(pool_[frame_id]->page_id);
    }
  }
  
  
  
}

void BufferManager::flush_pages(UNUSED_ATTRIBUTE uint64_t txn_id){
    // Flush all dirty pages acquired by the transaction to disk
    std::lock_guard<std::mutex> buffer_lock(file_use_mutex_);
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (lock_manager.txn_to_pages[txn_id].count(pool_[frame_id]->page_id) && pool_[frame_id]->dirty) {
        write_frame(pool_[frame_id]->frame_id);
        pool_[frame_id]->dirty = false;
      }
    }
    
    
    
}

void BufferManager::transaction_complete(UNUSED_ATTRIBUTE uint64_t txn_id){
  
  std::cout << txn_id << " complete" << std::endl;
    // Free all the locks acquired by the transaction
    flush_pages(txn_id);
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (lock_manager.txn_to_pages[txn_id].count(pool_[frame_id]->page_id)) {
        pool_[frame_id]->exclusive = false;
      }
    }
    for (auto page_id : lock_manager.txn_to_pages[txn_id]) {
      PageLock& lock = lock_manager.lock_table[page_id];
      std::unique_lock<std::mutex> lk(lock.mutex);
      lock.shared_holders.erase(txn_id);
      if (lock.exclusive_holder == txn_id) lock.exclusive_holder = 0;
      for (auto it = lock.queue.begin(); it != lock.queue.end(); ) {
        if (it->txn_id == txn_id) it = lock.queue.erase(it);
        else ++it;
      }
      for (auto& req : lock.queue) {
        req.cv.notify_all();
      }
    }
    lock_manager.txn_to_pages.erase(txn_id);
  
  
    
    
    
}

void BufferManager::transaction_abort(UNUSED_ATTRIBUTE uint64_t txn_id){

  
  std::cout << txn_id << " abort" << std::endl;
    // Free all the locks acquired by the transaction
    
    //discard_pages(txn_id);
      auto& txn_pages = lock_manager.txn_to_pages[txn_id];
      for (auto page_id : txn_pages) {
          auto& lock = lock_manager.lock_table[page_id];
          //std::unique_lock<std::mutex> lk(lock.mutex);
  
          for (auto it = lock.queue.begin(); it != lock.queue.end(); ) {
              if (it->txn_id == txn_id) {
                  it = lock.queue.erase(it);
              } else {
                  ++it;
              }
          }
  
          lock.shared_holders.erase(txn_id);
          if (lock.exclusive_holder == txn_id) {
              lock.exclusive_holder = 0;
          }
          
          if(lock.upgrading_txn_id == txn_id) lock.upgrading_txn_id = 0;
  
          for (auto& req : lock.queue) {
              req.cv.notify_all();
          }
      }
  
      lock_manager.txn_to_pages.erase(txn_id);
      lock_manager.waits_for.erase(txn_id);
  
  
}


void BufferManager::read_frame(uint64_t frame_id) {
  //std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
}

void BufferManager::write_frame(uint64_t frame_id) {
  //std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
}


}  // namespace buzzdb
