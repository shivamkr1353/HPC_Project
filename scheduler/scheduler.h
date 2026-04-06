#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "task/task.h"
#include "worker/worker.h"

namespace hybrid {

enum class AdaptivePolicy {
    WorkFirst,
    HelpFirst
};

struct SchedulerConfig {
    std::size_t worker_count{1};
    std::size_t locality_group_size{2};
    std::size_t cost_threshold{128};
    std::size_t imbalance_threshold{2};
    std::size_t share_batch_size{3};
    double steal_rate_threshold{0.08};
    double creation_rate_threshold{0.25};
    double creation_to_steal_ratio{1.4};
    double max_steal_rate_per_ms{0.40};
    std::size_t policy_refresh_ms{10};
    SchedulingMode mode{SchedulingMode::AdaptiveHybrid};
};

struct SchedulerMetrics {
    std::size_t executed_tasks{0};
    std::size_t steal_events{0};
    std::size_t share_events{0};
    std::vector<double> idle_ms_per_worker;
    AdaptivePolicy final_policy{AdaptivePolicy::WorkFirst};
    double steal_rate_per_ms{0.0};
    double creation_rate_per_ms{0.0};
};

class Scheduler {
   public:
    explicit Scheduler(SchedulerConfig config);
    ~Scheduler();

    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    template <typename Fn>
    auto submit(Fn&& fn, TaskMetadata metadata = {}) -> std::future<std::invoke_result_t<Fn>>;

    template <typename T>
    T await(std::future<T>& future);

    void wait_for_all();
    SchedulerMetrics snapshot_metrics();

    std::size_t worker_count() const { return workers_.empty() ? 1U : workers_.size(); }
    SchedulingMode mode() const { return config_.mode; }
    int current_worker_id() const;
    int current_locality_group() const;

    static std::string mode_name(SchedulingMode mode);
    static std::string policy_name(AdaptivePolicy policy);

   private:
    bool help_once(std::size_t worker_id);
    bool pop_and_run_local(std::size_t worker_id);
    bool try_steal(std::size_t thief_id);
    bool try_share(std::size_t donor_id, bool aggressive);
    void run_task(std::size_t worker_id, Task task);
    void worker_loop(std::size_t worker_id);

    int choose_enqueue_worker(const TaskMetadata& metadata, bool internal_submission);
    int choose_most_loaded_worker(std::size_t requester_id) const;
    int choose_least_loaded_worker(std::size_t donor_id) const;
    bool same_group(std::size_t lhs, std::size_t rhs) const;
    std::size_t load_score(std::size_t worker_id) const;
    bool should_allow_steal(std::size_t thief_id, std::size_t victim_id) const;
    bool should_share(std::size_t donor_id, std::size_t receiver_id, bool aggressive) const;
    std::size_t effective_share_batch(std::size_t donor_id, bool aggressive) const;
    void refresh_policy();
    void post_enqueue(std::size_t worker_id, bool internal_submission);
    void on_task_complete();

    static thread_local Scheduler* tls_scheduler_;
    static thread_local int tls_worker_id_;

    SchedulerConfig config_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::vector<std::thread> threads_;

    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;
    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;

    std::atomic<bool> stopping_{false};
    std::atomic<std::size_t> inflight_tasks_{0};
    std::atomic<std::size_t> submission_cursor_{0};
    std::atomic<std::size_t> tasks_created_{0};
    std::atomic<std::size_t> tasks_completed_{0};
    std::atomic<std::size_t> steals_{0};
    std::atomic<std::size_t> shares_{0};
    std::atomic<AdaptivePolicy> policy_{AdaptivePolicy::WorkFirst};

    mutable std::mutex policy_mutex_;
    std::chrono::steady_clock::time_point last_policy_refresh_;
    std::size_t last_refresh_created_{0};
    std::size_t last_refresh_steals_{0};
    double last_creation_rate_per_ms_{0.0};
    double last_steal_rate_per_ms_{0.0};
};

template <typename Fn>
auto Scheduler::submit(Fn&& fn, TaskMetadata metadata) -> std::future<std::invoke_result_t<Fn>> {
    using ResultType = std::invoke_result_t<Fn>;

    auto packaged_task =
        std::make_shared<std::packaged_task<ResultType()>>(std::forward<Fn>(fn));
    auto future = packaged_task->get_future();

    tasks_created_.fetch_add(1, std::memory_order_relaxed);
    if (config_.mode == SchedulingMode::Sequential || workers_.empty()) {
        (*packaged_task)();
        tasks_completed_.fetch_add(1, std::memory_order_relaxed);
        return future;
    }

    inflight_tasks_.fetch_add(1, std::memory_order_relaxed);

    Task task;
    task.metadata = metadata;
    task.execute = [packaged_task]() { (*packaged_task)(); };

    const bool internal_submission = current_worker_id() >= 0;
    const int target_worker = choose_enqueue_worker(metadata, internal_submission);
    workers_[static_cast<std::size_t>(target_worker)]->push(std::move(task));
    post_enqueue(static_cast<std::size_t>(target_worker), internal_submission);
    return future;
}

template <typename T>
T Scheduler::await(std::future<T>& future) {
    const int worker_id = current_worker_id();
    if (worker_id < 0 || config_.mode == SchedulingMode::Sequential) {
        if constexpr (std::is_void_v<T>) {
            future.get();
            return;
        } else {
            return future.get();
        }
    }

    while (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        if (!help_once(static_cast<std::size_t>(worker_id))) {
            std::this_thread::yield();
        }
    }

    if constexpr (std::is_void_v<T>) {
        future.get();
        return;
    } else {
        return future.get();
    }
}

}  // namespace hybrid
