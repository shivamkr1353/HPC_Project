#include "worker/worker.h"

#include <algorithm>
#include <utility>

namespace hybrid {

Worker::Worker(std::size_t id, std::size_t locality_group)
    : id_(id), locality_group_(locality_group) {}

void Worker::push(Task task) {
    std::lock_guard<std::mutex> lock(mutex_);
    queued_cost_.fetch_add(task.metadata.estimated_cost, std::memory_order_relaxed);
    deque_.push_back(std::move(task));
    queue_size_.store(deque_.size(), std::memory_order_relaxed);
}

void Worker::push_batch(std::vector<Task>&& tasks) {
    if (tasks.empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& task : tasks) {
        queued_cost_.fetch_add(task.metadata.estimated_cost, std::memory_order_relaxed);
        deque_.push_back(std::move(task));
    }
    queue_size_.store(deque_.size(), std::memory_order_relaxed);
}

bool Worker::pop_local(Task& task) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (deque_.empty()) {
        return false;
    }

    task = std::move(deque_.back());
    deque_.pop_back();
    queued_cost_.fetch_sub(task.metadata.estimated_cost, std::memory_order_relaxed);
    queue_size_.store(deque_.size(), std::memory_order_relaxed);
    return true;
}

bool Worker::steal_front(Task& task, std::size_t cost_threshold, bool enforce_cost) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (deque_.empty()) {
        return false;
    }

    if (enforce_cost && deque_.front().metadata.estimated_cost < cost_threshold) {
        return false;
    }

    task = std::move(deque_.front());
    deque_.pop_front();
    queued_cost_.fetch_sub(task.metadata.estimated_cost, std::memory_order_relaxed);
    queue_size_.store(deque_.size(), std::memory_order_relaxed);
    return true;
}

std::vector<Task> Worker::extract_share_batch(std::size_t max_count,
                                              std::size_t cost_threshold,
                                              bool enforce_cost) {
    std::vector<Task> batch;
    if (max_count == 0) {
        return batch;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (deque_.size() < 2) {
        return batch;
    }

    const std::size_t target_count = std::min(max_count, deque_.size() / 2);
    batch.reserve(target_count);
    for (std::size_t index = 0; index < target_count; ++index) {
        if (enforce_cost && deque_.front().metadata.estimated_cost < cost_threshold) {
            break;
        }

        Task task = std::move(deque_.front());
        deque_.pop_front();
        queued_cost_.fetch_sub(task.metadata.estimated_cost, std::memory_order_relaxed);
        batch.push_back(std::move(task));
    }

    queue_size_.store(deque_.size(), std::memory_order_relaxed);
    return batch;
}

void Worker::add_idle_time(std::chrono::nanoseconds idle_time) {
    idle_time_ns_.fetch_add(idle_time.count(), std::memory_order_relaxed);
}

double Worker::idle_time_ms() const {
    return static_cast<double>(idle_time_ns_.load(std::memory_order_relaxed)) / 1'000'000.0;
}

void Worker::mark_task_executed() {
    executed_tasks_.fetch_add(1, std::memory_order_relaxed);
}

void Worker::mark_steal() {
    steal_events_.fetch_add(1, std::memory_order_relaxed);
}

void Worker::mark_share() {
    share_events_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace hybrid
