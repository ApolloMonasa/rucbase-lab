/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::scoped_lock lock{latch_};
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto &queue = lock_table_[lock_data_id];

    txn_id_t tid = txn->get_transaction_id();
    // 检查是否已持有相同或更强的锁，避免重复加锁
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ == tid) {
            if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;  // 已持有 S 或 X 锁，无需重复加 S 锁
            }
        }
    }
    // 检查是否与其他事务的已授予锁冲突（同一事务不冲突，支持锁升级）
    for (auto &req : queue.request_queue_) {
        if (!req.granted_ || req.txn_id_ == tid) continue;
        // S 锁与 X, SIX 冲突
        if (req.lock_mode_ == LockMode::EXLUCSIVE || req.lock_mode_ == LockMode::S_IX) {
            throw TransactionAbortException(tid, AbortReason::DEADLOCK_PREVENTION);
        }
    }
    queue.request_queue_.emplace_back(tid, LockMode::SHARED);
    queue.request_queue_.back().granted_ = true;
    // 更新队列锁模式：S 可能提升当前模式但不超过 S
    if (queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        queue.group_lock_mode_ == GroupLockMode::IS ||
        queue.group_lock_mode_ == GroupLockMode::IX) {
        queue.group_lock_mode_ = GroupLockMode::S;
    }
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::scoped_lock lock{latch_};
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto &queue = lock_table_[lock_data_id];

    txn_id_t tid = txn->get_transaction_id();
    // 检查是否已持有相同或更强的锁，避免重复加锁
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ == tid && req.lock_mode_ == LockMode::EXLUCSIVE) {
            return true;  // 已持有 X 锁，无需重复加 X 锁
        }
    }
    // 检查是否与其他事务的已授予锁冲突（同一事务不冲突，支持锁升级）
    for (auto &req : queue.request_queue_) {
        if (!req.granted_ || req.txn_id_ == tid) continue;
        // X 锁与任何其他锁都冲突
        throw TransactionAbortException(tid, AbortReason::DEADLOCK_PREVENTION);
    }
    queue.request_queue_.emplace_back(tid, LockMode::EXLUCSIVE);
    queue.request_queue_.back().granted_ = true;
    queue.group_lock_mode_ = GroupLockMode::X;
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::scoped_lock lock{latch_};
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_data_id];

    txn_id_t tid = txn->get_transaction_id();
    // 检查是否已持有相同或更强的锁，避免重复加锁
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ == tid) {
            if (req.lock_mode_ == LockMode::SHARED ||
                req.lock_mode_ == LockMode::EXLUCSIVE ||
                req.lock_mode_ == LockMode::S_IX) {
                return true;  // 已持有 S, X 或 SIX 锁，无需重复加 S 锁
            }
        }
    }
    // 检查是否与其他事务的已授予锁冲突（同一事务不冲突，支持锁升级）
    for (auto &req : queue.request_queue_) {
        if (!req.granted_ || req.txn_id_ == tid) continue;
        // S 锁与 X, SIX, IX 冲突（表级 S 与 IX 也不相容）
        if (req.lock_mode_ == LockMode::EXLUCSIVE ||
            req.lock_mode_ == LockMode::S_IX ||
            req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
            throw TransactionAbortException(tid, AbortReason::DEADLOCK_PREVENTION);
        }
    }
    queue.request_queue_.emplace_back(tid, LockMode::SHARED);
    queue.request_queue_.back().granted_ = true;
    // 更新队列锁模式
    if (static_cast<int>(queue.group_lock_mode_) < static_cast<int>(GroupLockMode::S)) {
        queue.group_lock_mode_ = GroupLockMode::S;
    } else if (queue.group_lock_mode_ == GroupLockMode::IX) {
        queue.group_lock_mode_ = GroupLockMode::SIX;
    }
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::scoped_lock lock{latch_};
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_data_id];

    txn_id_t tid = txn->get_transaction_id();
    // 检查是否已持有相同或更强的锁，避免重复加锁
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ == tid && req.lock_mode_ == LockMode::EXLUCSIVE) {
            return true;  // 已持有 X 锁，无需重复加 X 锁
        }
    }
    // 检查是否与其他事务的已授予锁冲突（同一事务不冲突，支持锁升级）
    for (auto &req : queue.request_queue_) {
        if (!req.granted_ || req.txn_id_ == tid) continue;
        // X 锁与任何其他锁都冲突
        throw TransactionAbortException(tid, AbortReason::DEADLOCK_PREVENTION);
    }
    queue.request_queue_.emplace_back(tid, LockMode::EXLUCSIVE);
    queue.request_queue_.back().granted_ = true;
    queue.group_lock_mode_ = GroupLockMode::X;
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::scoped_lock lock{latch_};
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_data_id];

    txn_id_t tid = txn->get_transaction_id();
    // 检查是否已持有任何表级锁，避免重复加锁（IS 是最弱的意向锁）
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ == tid) {
            return true;  // 已持有表级锁，无需重复加 IS 锁
        }
    }
    // 检查是否与其他事务的已授予锁冲突（同一事务不冲突，支持锁升级）
    for (auto &req : queue.request_queue_) {
        if (!req.granted_ || req.txn_id_ == tid) continue;
        // IS 锁只与 X 冲突
        if (req.lock_mode_ == LockMode::EXLUCSIVE) {
            throw TransactionAbortException(tid, AbortReason::DEADLOCK_PREVENTION);
        }
    }
    queue.request_queue_.emplace_back(tid, LockMode::INTENTION_SHARED);
    queue.request_queue_.back().granted_ = true;
    if (queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        queue.group_lock_mode_ = GroupLockMode::IS;
    }
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::scoped_lock lock{latch_};
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_data_id];

    txn_id_t tid = txn->get_transaction_id();
    // 检查是否已持有相同或更强的锁，避免重复加锁
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ == tid) {
            if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                req.lock_mode_ == LockMode::S_IX ||
                req.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;  // 已持有 IX, SIX 或 X 锁，无需重复加 IX 锁
            }
        }
    }
    // 检查是否与其他事务的已授予锁冲突（同一事务不冲突，支持锁升级）
    for (auto &req : queue.request_queue_) {
        if (!req.granted_ || req.txn_id_ == tid) continue;
        // IX 锁与 S, X, SIX 冲突
        if (req.lock_mode_ == LockMode::SHARED ||
            req.lock_mode_ == LockMode::EXLUCSIVE ||
            req.lock_mode_ == LockMode::S_IX) {
            throw TransactionAbortException(tid, AbortReason::DEADLOCK_PREVENTION);
        }
    }
    queue.request_queue_.emplace_back(tid, LockMode::INTENTION_EXCLUSIVE);
    queue.request_queue_.back().granted_ = true;
    if (static_cast<int>(queue.group_lock_mode_) < static_cast<int>(GroupLockMode::IX)) {
        queue.group_lock_mode_ = GroupLockMode::IX;
    } else if (queue.group_lock_mode_ == GroupLockMode::S) {
        queue.group_lock_mode_ = GroupLockMode::SIX;
    }
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::scoped_lock lock{latch_};
    auto it = lock_table_.find(lock_data_id);
    if (it == lock_table_.end()) {
        return false;
    }
    auto &queue = it->second;
    txn_id_t tid = txn->get_transaction_id();
    // 移除该事务在此 LockDataId 上的所有已授予锁（同一事务可能有多次加锁，如先S后X的锁升级）
    auto req_it = queue.request_queue_.begin();
    while (req_it != queue.request_queue_.end()) {
        if (req_it->txn_id_ == tid && req_it->granted_) {
            req_it = queue.request_queue_.erase(req_it);
        } else {
            ++req_it;
        }
    }
    if (queue.request_queue_.empty()) {
        lock_table_.erase(it);
    } else {
        // 重新计算队列的 GroupLockMode
        queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
        for (auto &req : queue.request_queue_) {
            if (!req.granted_) continue;
            GroupLockMode m = GroupLockMode::NON_LOCK;
            switch (req.lock_mode_) {
                case LockMode::SHARED:              m = GroupLockMode::S; break;
                case LockMode::EXLUCSIVE:           m = GroupLockMode::X; break;
                case LockMode::INTENTION_SHARED:    m = GroupLockMode::IS; break;
                case LockMode::INTENTION_EXCLUSIVE: m = GroupLockMode::IX; break;
                case LockMode::S_IX:                m = GroupLockMode::SIX; break;
            }
            if (static_cast<int>(queue.group_lock_mode_) < static_cast<int>(m)) {
                queue.group_lock_mode_ = m;
            }
        }
    }
    return true;
}
