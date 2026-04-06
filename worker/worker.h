#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>

#include "task/task.h"

namespace hybrid {

class Worker {
   public:
    Worker(std::size_t id, std::size_t locality_group);

    void push(Task task);
    void push_batch(std::vector<Task>&& tasks);

    bool pop_local(Task& task);
    bool steal_front(Task& task, std::size_t cost_threshold, bool enforce_cost);
    std::vector<Task> extract_share_batch(std::size_t max_count,
                                          std::size_t cost_threshold,
                                          bool enforce_cost);

    std::size_t id() const { return id_; }
    std::size_t locality_group() const { return locality_group_; }
    std::size_t queue_size() const { return queue_size_.load(std::memory_order_relaxed); }
    std::size_t queued_cost() const { return queued_cost_.load(std::memory_order_relaxed); }

    void add_idle_time(std::chrono::nanoseconds idle_time);
    double idle_time_ms() const;

    void mark_task_executed();
    void mark_steal();
    void mark_share();

    std::size_t executed_tasks() const { return executed_tasks_.load(std::memory_order_relaxed); }
    std::size_t steal_events() const { return steal_events_.load(std::memory_order_relaxed); }
    std::size_t share_events() const { return share_events_.load(std::memory_order_relaxed); }

   private:
    std::size_t id_;
    std::size_t locality_group_;

    mutable std::mutex mutex_;
    std::deque<Task> deque_;

    std::atomic<std::size_t> queue_size_{0};
    std::atomic<std::size_t> queued_cost_{0};
    std::atomic<long long> idle_time_ns_{0};
    std::atomic<std::size_t> executed_tasks_{0};
    std::atomic<std::size_t> steal_events_{0};
    std::atomic<std::size_t> share_events_{0};
};

}  // namespace hybrid
