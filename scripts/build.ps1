$ErrorActionPreference = "Stop"

$sources = @(
    "main.cpp",
    "scheduler/scheduler.cpp",
    "worker/worker.cpp",
    "task/workloads.cpp"
)

g++ -std=c++17 -O2 -Wall -Wextra -I. $sources -o hybrid_scheduler.exe
Write-Host "Built hybrid_scheduler.exe"
