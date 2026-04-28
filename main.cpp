#include <algorithm>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "scheduler/scheduler.h"
#include "task/workloads.h"

namespace {

using hybrid::Scheduler;
using hybrid::SchedulerConfig;
using hybrid::SchedulerMetrics;
using hybrid::SchedulingMode;

struct CliOptions {
    std::string output_csv{"results.csv"};
    std::vector<std::size_t> thread_counts;
    std::size_t merge_size{1U << 18};
    std::size_t matrix_size{256};
    int fib_n{35};
};

struct BenchmarkSpec {
    std::string name;
    std::string problem_size;
    std::function<std::string(Scheduler&)> run;
    std::function<std::string(std::size_t)> run_openmp;
};

struct CsvRow {
    std::string workload;
    std::string mode;
    std::size_t threads{1};
    std::string problem_size;
    double execution_ms{0.0};
    double speedup{0.0};
    double efficiency{0.0};
    std::size_t steals{0};
    std::size_t shares{0};
    std::string idle_ms_per_worker;
    std::string policy;
    double steal_rate_per_ms{0.0};
    double creation_rate_per_ms{0.0};
    std::string validation;
};

std::string escape_csv(const std::string& value) {
    if (value.find_first_of(",\"") == std::string::npos) {
        return value;
    }

    std::string escaped = "\"";
    for (char ch : value) {
        if (ch == '"') {
            escaped += "\"\"";
        } else {
            escaped += ch;
        }
    }
    escaped += "\"";
    return escaped;
}

std::vector<std::size_t> parse_threads(const std::string& value) {
    std::vector<std::size_t> result;
    std::stringstream stream(value);
    std::string token;
    while (std::getline(stream, token, ',')) {
        if (!token.empty()) {
            result.push_back(static_cast<std::size_t>(std::stoul(token)));
        }
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

std::vector<std::size_t> default_thread_counts() {
    const std::size_t hardware_threads =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());
    std::set<std::size_t> values{1};
    if (hardware_threads >= 2) {
        values.insert(2);
    }
    if (hardware_threads >= 4) {
        values.insert(4);
    }
    return {values.begin(), values.end()};
}

CliOptions parse_cli(int argc, char** argv) {
    CliOptions options;
    options.thread_counts = default_thread_counts();

    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        if (argument == "--output" && index + 1 < argc) {
            options.output_csv = argv[++index];
        } else if (argument == "--threads" && index + 1 < argc) {
            options.thread_counts = parse_threads(argv[++index]);
        } else if (argument == "--merge-size" && index + 1 < argc) {
            options.merge_size = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else if (argument == "--matrix-size" && index + 1 < argc) {
            options.matrix_size = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else if (argument == "--fib-n" && index + 1 < argc) {
            options.fib_n = std::stoi(argv[++index]);
        }
    }

    if (options.thread_counts.empty()) {
        options.thread_counts = default_thread_counts();
    }
    return options;
}

std::string join_idle_times(const std::vector<double>& idle_times) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(3);
    for (std::size_t index = 0; index < idle_times.size(); ++index) {
        if (index > 0) {
            stream << ';';
        }
        stream << "w" << index << '=' << idle_times[index];
    }
    return stream.str();
}

void write_csv(const std::string& output_csv, const std::vector<CsvRow>& rows) {
    std::ofstream output(output_csv, std::ios::trunc);
    output << "workload,mode,threads,problem_size,execution_ms,speedup,efficiency,steals,shares,"
              "idle_ms_per_worker,policy,steal_rate_per_ms,creation_rate_per_ms,validation\n";
    output << std::fixed << std::setprecision(4);
    for (const auto& row : rows) {
        output << escape_csv(row.workload) << ','
               << escape_csv(row.mode) << ','
               << row.threads << ','
               << escape_csv(row.problem_size) << ','
               << row.execution_ms << ','
               << row.speedup << ','
               << row.efficiency << ','
               << row.steals << ','
               << row.shares << ','
               << escape_csv(row.idle_ms_per_worker) << ','
               << escape_csv(row.policy) << ','
               << row.steal_rate_per_ms << ','
               << row.creation_rate_per_ms << ','
               << escape_csv(row.validation) << '\n';
    }
}

CsvRow run_benchmark(const BenchmarkSpec& spec,
                     SchedulingMode mode,
                     std::size_t threads,
                     double baseline_ms) {
    SchedulerConfig config;
    config.worker_count = threads;
    config.locality_group_size = std::max<std::size_t>(1, threads / 2);
    config.mode = mode;

    Scheduler scheduler(config);
    const auto start = std::chrono::steady_clock::now();
    const std::string validation = spec.run(scheduler);
    scheduler.wait_for_all();
    const auto finish = std::chrono::steady_clock::now();
    SchedulerMetrics metrics = scheduler.snapshot_metrics();

    const double execution_ms =
        std::chrono::duration<double, std::milli>(finish - start).count();
    const double speedup = baseline_ms / execution_ms;
    const double efficiency = speedup / static_cast<double>(std::max<std::size_t>(1, threads));

    CsvRow row;
    row.workload = spec.name;
    row.mode = Scheduler::mode_name(mode);
    row.threads = threads;
    row.problem_size = spec.problem_size;
    row.execution_ms = execution_ms;
    row.speedup = speedup;
    row.efficiency = efficiency;
    row.steals = metrics.steal_events;
    row.shares = metrics.share_events;
    row.idle_ms_per_worker = join_idle_times(metrics.idle_ms_per_worker);
    row.policy = Scheduler::policy_name(metrics.final_policy);
    row.steal_rate_per_ms = metrics.steal_rate_per_ms;
    row.creation_rate_per_ms = metrics.creation_rate_per_ms;
    row.validation = validation;
    return row;
}

void print_row(const CsvRow& row) {
    std::cout << std::left << std::setw(15) << row.workload
              << std::setw(18) << row.mode
              << std::setw(8) << row.threads
              << std::setw(13) << std::fixed << std::setprecision(2) << row.execution_ms
              << std::setw(11) << std::setprecision(2) << row.speedup
              << std::setw(11) << std::setprecision(2) << row.efficiency
              << std::setw(9) << row.steals
              << std::setw(9) << row.shares
              << row.policy << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    const CliOptions options = parse_cli(argc, argv);

    const std::vector<BenchmarkSpec> benchmarks{
        {
            "MergeSort",
            "n=" + std::to_string(options.merge_size),
            [&options](Scheduler& scheduler) {
                return std::to_string(hybrid::run_parallel_merge_sort(scheduler, options.merge_size));
            },
            [&options](std::size_t threads) {
                return std::to_string(hybrid::run_openmp_parallel_merge_sort(options.merge_size, threads));
            },
        },
        {
            "MatrixMul",
            "n=" + std::to_string(options.matrix_size),
            [&options](Scheduler& scheduler) {
                std::ostringstream stream;
                stream << std::fixed << std::setprecision(6)
                       << hybrid::run_matrix_multiplication(scheduler, options.matrix_size);
                return stream.str();
            },
            [&options](std::size_t threads) {
                std::ostringstream stream;
                stream << std::fixed << std::setprecision(6)
                       << hybrid::run_openmp_matrix_multiplication(options.matrix_size, threads);
                return stream.str();
            },
        },
        {
            "Fibonacci",
            "n=" + std::to_string(options.fib_n),
            [&options](Scheduler& scheduler) {
                return std::to_string(hybrid::run_recursive_fibonacci(scheduler, options.fib_n));
            },
            [&options](std::size_t threads) {
                return std::to_string(hybrid::run_openmp_recursive_fibonacci(options.fib_n, threads));
            },
        },
    };

    std::vector<CsvRow> rows;
    rows.reserve(benchmarks.size() * (options.thread_counts.size() * 3 + 1));
    std::unordered_map<std::string, double> baselines_ms;

    std::cout << std::left << std::setw(15) << "Workload"
              << std::setw(18) << "Mode"
              << std::setw(8) << "Threads"
              << std::setw(13) << "Time(ms)"
              << std::setw(11) << "Speedup"
              << std::setw(11) << "Eff."
              << std::setw(9) << "Steals"
              << std::setw(9) << "Shares"
              << "Policy\n";
    std::cout << std::string(100, '-') << '\n';

    for (const auto& benchmark : benchmarks) {
        CsvRow baseline = run_benchmark(benchmark, SchedulingMode::Sequential, 1, 1.0);
        baseline.speedup = 1.0;
        baseline.efficiency = 1.0;
        baselines_ms[benchmark.name] = baseline.execution_ms;
        rows.push_back(baseline);
        print_row(baseline);

        const std::vector<SchedulingMode> modes{
            SchedulingMode::Static,
            SchedulingMode::WorkStealing,
            SchedulingMode::AdaptiveHybrid,
        };

        for (SchedulingMode mode : modes) {
            for (std::size_t threads : options.thread_counts) {
                CsvRow row =
                    run_benchmark(benchmark, mode, threads, baselines_ms[benchmark.name]);
                rows.push_back(row);
                print_row(row);
            }
        }

        for (std::size_t threads : options.thread_counts) {
            const auto start = std::chrono::steady_clock::now();
            std::string validation = benchmark.run_openmp(threads);
            const auto finish = std::chrono::steady_clock::now();
            const double execution_ms =
                std::chrono::duration<double, std::milli>(finish - start).count();
            const double speedup = baselines_ms[benchmark.name] / execution_ms;
            const double efficiency = speedup / static_cast<double>(std::max<std::size_t>(1, threads));

            CsvRow row;
            row.workload = benchmark.name;
            row.mode = "OpenMP";
            row.threads = threads;
            row.problem_size = benchmark.problem_size;
            row.execution_ms = execution_ms;
            row.speedup = speedup;
            row.efficiency = efficiency;
            row.steals = 0;
            row.shares = 0;
            row.idle_ms_per_worker = "";
            row.policy = "OpenMP";
            row.steal_rate_per_ms = 0.0;
            row.creation_rate_per_ms = 0.0;
            row.validation = validation;

            rows.push_back(row);
            print_row(row);
        }
    }

    write_csv(options.output_csv, rows);
    std::cout << "\nResults saved to " << options.output_csv << '\n';
    return 0;
}
