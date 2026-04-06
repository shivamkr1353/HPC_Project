#include "scheduler/scheduler.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace hybrid {

thread_local Scheduler* Scheduler::tls_scheduler_ = nullptr;
thread_local int Scheduler::tls_worker_id_ = -1;

Scheduler::Scheduler(SchedulerConfig config) : config_(config) {
    if (config_.worker_count == 0) {
        config_.worker_count = 1;
    }
    if (config_.locality_group_size == 0) {
        config_.locality_group_size = 1;
    }

    last_policy_refresh_ = std::chrono::steady_clock::now();

    if (config_.mode == SchedulingMode::Sequential) {
        return;
    }

    workers_.reserve(config_.worker_count);
    for (std::size_t worker_id = 0; worker_id < config_.worker_count; ++worker_id) {
        workers_.push_back(std::make_unique<Worker>(worker_id,
                                                    worker_id / config_.locality_group_size));
    }

    threads_.reserve(config_.worker_count);
    for (std::size_t worker_id = 0; worker_id < config_.worker_count; ++worker_id) {
        threads_.emplace_back(&Scheduler::worker_loop, this, worker_id);
    }
}

Scheduler::~Scheduler() {
    wait_for_all();
    stopping_.store(true, std::memory_order_relaxed);
    wake_cv_.notify_all();

    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void Scheduler::wait_for_all() {
    if (config_.mode == SchedulingMode::Sequential || workers_.empty()) {
        return;
    }

    std::unique_lock<std::mutex> lock(completion_mutex_);
    completion_cv_.wait(lock, [this]() {
        return inflight_tasks_.load(std::memory_order_relaxed) == 0;
    });
}

SchedulerMetrics Scheduler::snapshot_metrics() {
    refresh_policy();

    SchedulerMetrics metrics;
    metrics.executed_tasks = tasks_completed_.load(std::memory_order_relaxed);
    metrics.steal_events = steals_.load(std::memory_order_relaxed);
    metrics.share_events = shares_.load(std::memory_order_relaxed);
    metrics.final_policy = policy_.load(std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lock(policy_mutex_);
        metrics.steal_rate_per_ms = last_steal_rate_per_ms_;
        metrics.creation_rate_per_ms = last_creation_rate_per_ms_;
    }

    if (workers_.empty()) {
        metrics.idle_ms_per_worker.push_back(0.0);
        return metrics;
    }

    metrics.idle_ms_per_worker.reserve(workers_.size());
    for (const auto& worker : workers_) {
        metrics.idle_ms_per_worker.push_back(worker->idle_time_ms());
    }
    return metrics;
}

int Scheduler::current_worker_id() const {
    if (tls_scheduler_ == this) {
        return tls_worker_id_;
    }
    return -1;
}

int Scheduler::current_locality_group() const {
    const int worker_id = current_worker_id();
    if (worker_id < 0) {
        return -1;
    }
    return static_cast<int>(workers_[static_cast<std::size_t>(worker_id)]->locality_group());
}

std::string Scheduler::mode_name(SchedulingMode mode) {
    switch (mode) {
        case SchedulingMode::Sequential:
            return "Sequential";
        case SchedulingMode::Static:
            return "Static";
        case SchedulingMode::WorkStealing:
            return "WorkStealing";
        case SchedulingMode::AdaptiveHybrid:
            return "AdaptiveHybrid";
    }
    return "Unknown";
}

std::string Scheduler::policy_name(AdaptivePolicy policy) {
    switch (policy) {
        case AdaptivePolicy::WorkFirst:
            return "WorkFirst";
        case AdaptivePolicy::HelpFirst:
            return "HelpFirst";
    }
    return "Unknown";
}

bool Scheduler::help_once(std::size_t worker_id) {
    if (pop_and_run_local(worker_id)) {
        return true;
    }

    if (config_.mode == SchedulingMode::WorkStealing ||
        config_.mode == SchedulingMode::AdaptiveHybrid) {
        return try_steal(worker_id);
    }

    return false;
}

bool Scheduler::pop_and_run_local(std::size_t worker_id) {
    if (workers_.empty()) {
        return false;
    }

    Task task;
    if (!workers_[worker_id]->pop_local(task)) {
        return false;
    }

    run_task(worker_id, std::move(task));
    return true;
}

bool Scheduler::try_steal(std::size_t thief_id) {
    if (workers_.size() < 2) {
        return false;
    }

    refresh_policy();
    const int victim_id = choose_most_loaded_worker(thief_id);
    if (victim_id < 0) {
        return false;
    }

    if (!should_allow_steal(thief_id, static_cast<std::size_t>(victim_id))) {
        return false;
    }

    Task stolen_task;
    const bool enforce_cost = config_.mode == SchedulingMode::AdaptiveHybrid;
    if (!workers_[static_cast<std::size_t>(victim_id)]->steal_front(stolen_task,
                                                                    config_.cost_threshold,
                                                                    enforce_cost)) {
        return false;
    }

    steals_.fetch_add(1, std::memory_order_relaxed);
    workers_[thief_id]->mark_steal();
    run_task(thief_id, std::move(stolen_task));
    return true;
}

bool Scheduler::try_share(std::size_t donor_id, bool aggressive) {
    if (config_.mode != SchedulingMode::AdaptiveHybrid || workers_.size() < 2) {
        return false;
    }

    const int receiver_id = choose_least_loaded_worker(donor_id);
    if (receiver_id < 0) {
        return false;
    }

    if (!should_share(donor_id, static_cast<std::size_t>(receiver_id), aggressive)) {
        return false;
    }

    const std::size_t batch_size = effective_share_batch(donor_id, aggressive);
    const std::size_t cost_threshold =
        aggressive ? std::max<std::size_t>(1, config_.cost_threshold / 2) : config_.cost_threshold;
    auto shared_tasks =
        workers_[donor_id]->extract_share_batch(batch_size, cost_threshold, true);
    if (shared_tasks.empty()) {
        return false;
    }

    workers_[static_cast<std::size_t>(receiver_id)]->push_batch(std::move(shared_tasks));
    shares_.fetch_add(1, std::memory_order_relaxed);
    workers_[donor_id]->mark_share();
    wake_cv_.notify_all();
    return true;
}

void Scheduler::run_task(std::size_t worker_id, Task task) {
    task.execute();
    workers_[worker_id]->mark_task_executed();
    on_task_complete();
}

void Scheduler::worker_loop(std::size_t worker_id) {
    tls_scheduler_ = this;
    tls_worker_id_ = static_cast<int>(worker_id);

    while (true) {
        refresh_policy();

        if (pop_and_run_local(worker_id)) {
            continue;
        }

        if ((config_.mode == SchedulingMode::WorkStealing ||
             config_.mode == SchedulingMode::AdaptiveHybrid) &&
            try_steal(worker_id)) {
            continue;
        }

        if (stopping_.load(std::memory_order_relaxed) &&
            inflight_tasks_.load(std::memory_order_relaxed) == 0) {
            break;
        }

        const auto idle_start = std::chrono::steady_clock::now();
        std::unique_lock<std::mutex> lock(wake_mutex_);
        wake_cv_.wait_for(lock, std::chrono::milliseconds(1));
        const auto idle_end = std::chrono::steady_clock::now();
        workers_[worker_id]->add_idle_time(idle_end - idle_start);
    }

    tls_worker_id_ = -1;
    tls_scheduler_ = nullptr;
}

int Scheduler::choose_enqueue_worker(const TaskMetadata& metadata, bool internal_submission) {
    if (workers_.empty()) {
        return 0;
    }

    if (!internal_submission) {
        const std::size_t next_worker =
            submission_cursor_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        return static_cast<int>(next_worker);
    }

    const int local_worker = current_worker_id();
    if (local_worker < 0) {
        const std::size_t next_worker =
            submission_cursor_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        return static_cast<int>(next_worker);
    }

    if (config_.mode == SchedulingMode::Static ||
        config_.mode == SchedulingMode::WorkStealing) {
        return local_worker;
    }

    refresh_policy();
    const int least_loaded = choose_least_loaded_worker(static_cast<std::size_t>(local_worker));
    if (least_loaded >= 0) {
        const std::size_t local_queue =
            workers_[static_cast<std::size_t>(local_worker)]->queue_size();
        const std::size_t remote_queue =
            workers_[static_cast<std::size_t>(least_loaded)]->queue_size();
        const bool is_expensive = metadata.estimated_cost >= config_.cost_threshold;
        const bool overloaded =
            local_queue >= remote_queue + config_.imbalance_threshold;
        const bool help_first =
            policy_.load(std::memory_order_relaxed) == AdaptivePolicy::HelpFirst;

        if (is_expensive && overloaded && (help_first || local_queue > config_.share_batch_size)) {
            return least_loaded;
        }
    }

    return local_worker;
}

int Scheduler::choose_most_loaded_worker(std::size_t requester_id) const {
    int selected_worker = -1;
    std::size_t selected_load = 0;

    for (int pass = 0; pass < 2; ++pass) {
        for (std::size_t worker_id = 0; worker_id < workers_.size(); ++worker_id) {
            if (worker_id == requester_id) {
                continue;
            }
            if (pass == 0 && !same_group(requester_id, worker_id)) {
                continue;
            }

            const std::size_t score = load_score(worker_id);
            if (score > selected_load) {
                selected_load = score;
                selected_worker = static_cast<int>(worker_id);
            }
        }
        if (selected_worker >= 0) {
            break;
        }
    }

    return selected_worker;
}

int Scheduler::choose_least_loaded_worker(std::size_t donor_id) const {
    int selected_worker = -1;
    std::size_t selected_load = std::numeric_limits<std::size_t>::max();

    for (int pass = 0; pass < 2; ++pass) {
        for (std::size_t worker_id = 0; worker_id < workers_.size(); ++worker_id) {
            if (worker_id == donor_id) {
                continue;
            }
            if (pass == 0 && !same_group(donor_id, worker_id)) {
                continue;
            }

            const std::size_t score = load_score(worker_id);
            if (score < selected_load) {
                selected_load = score;
                selected_worker = static_cast<int>(worker_id);
            }
        }
        if (selected_worker >= 0) {
            break;
        }
    }

    return selected_worker;
}

bool Scheduler::same_group(std::size_t lhs, std::size_t rhs) const {
    return workers_[lhs]->locality_group() == workers_[rhs]->locality_group();
}

std::size_t Scheduler::load_score(std::size_t worker_id) const {
    return workers_[worker_id]->queued_cost() + workers_[worker_id]->queue_size() * 8U;
}

bool Scheduler::should_allow_steal(std::size_t thief_id, std::size_t victim_id) const {
    const std::size_t victim_queue = workers_[victim_id]->queue_size();
    if (victim_queue == 0) {
        return false;
    }

    if (config_.mode != SchedulingMode::AdaptiveHybrid) {
        return true;
    }

    const std::size_t thief_queue = workers_[thief_id]->queue_size();
    const std::size_t imbalance =
        victim_queue > thief_queue ? victim_queue - thief_queue : 0U;
    if (imbalance < config_.imbalance_threshold) {
        return false;
    }

    std::lock_guard<std::mutex> lock(policy_mutex_);
    if (last_steal_rate_per_ms_ > config_.max_steal_rate_per_ms &&
        policy_.load(std::memory_order_relaxed) == AdaptivePolicy::WorkFirst) {
        return false;
    }

    return true;
}

bool Scheduler::should_share(std::size_t donor_id,
                             std::size_t receiver_id,
                             bool aggressive) const {
    if (donor_id == receiver_id) {
        return false;
    }

    const std::size_t donor_queue = workers_[donor_id]->queue_size();
    const std::size_t receiver_queue = workers_[receiver_id]->queue_size();
    if (donor_queue < 2) {
        return false;
    }

    const std::size_t imbalance =
        donor_queue > receiver_queue ? donor_queue - receiver_queue : 0U;
    const std::size_t required_imbalance =
        aggressive ? config_.imbalance_threshold : config_.imbalance_threshold + 1U;
    if (imbalance < required_imbalance) {
        return false;
    }

    if (!aggressive && workers_[donor_id]->queued_cost() < config_.cost_threshold) {
        return false;
    }

    return load_score(donor_id) > load_score(receiver_id);
}

std::size_t Scheduler::effective_share_batch(std::size_t donor_id, bool aggressive) const {
    const std::size_t queue_size = workers_[donor_id]->queue_size();
    const std::size_t requested_batch = config_.share_batch_size + (aggressive ? 1U : 0U);
    return std::max<std::size_t>(1, std::min(requested_batch, std::max<std::size_t>(1, queue_size / 3)));
}

void Scheduler::refresh_policy() {
    if (config_.mode != SchedulingMode::AdaptiveHybrid) {
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(policy_mutex_);
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - last_policy_refresh_).count();
    if (elapsed < static_cast<long long>(config_.policy_refresh_ms)) {
        return;
    }

    const std::size_t created = tasks_created_.load(std::memory_order_relaxed);
    const std::size_t steals = steals_.load(std::memory_order_relaxed);
    last_creation_rate_per_ms_ =
        static_cast<double>(created - last_refresh_created_) / static_cast<double>(elapsed);
    last_steal_rate_per_ms_ =
        static_cast<double>(steals - last_refresh_steals_) / static_cast<double>(elapsed);

    AdaptivePolicy next_policy = policy_.load(std::memory_order_relaxed);
    if (last_steal_rate_per_ms_ >= config_.steal_rate_threshold &&
        last_steal_rate_per_ms_ * 1.25 >=
            std::max(config_.creation_rate_threshold, last_creation_rate_per_ms_)) {
        next_policy = AdaptivePolicy::HelpFirst;
    } else if (last_creation_rate_per_ms_ >=
               std::max(0.01, last_steal_rate_per_ms_ * config_.creation_to_steal_ratio)) {
        next_policy = AdaptivePolicy::WorkFirst;
    } else if (last_steal_rate_per_ms_ > last_creation_rate_per_ms_) {
        next_policy = AdaptivePolicy::HelpFirst;
    }

    policy_.store(next_policy, std::memory_order_relaxed);
    last_refresh_created_ = created;
    last_refresh_steals_ = steals;
    last_policy_refresh_ = now;
}

void Scheduler::post_enqueue(std::size_t worker_id, bool internal_submission) {
    wake_cv_.notify_all();
    if (config_.mode == SchedulingMode::AdaptiveHybrid && internal_submission) {
        refresh_policy();
        const bool aggressive =
            policy_.load(std::memory_order_relaxed) == AdaptivePolicy::HelpFirst;
        static_cast<void>(try_share(worker_id, aggressive));
    }
}

void Scheduler::on_task_complete() {
    tasks_completed_.fetch_add(1, std::memory_order_relaxed);
    if (inflight_tasks_.fetch_sub(1, std::memory_order_relaxed) == 1) {
        completion_cv_.notify_all();
    }
}

}  // namespace hybrid
