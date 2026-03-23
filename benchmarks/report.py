#!/usr/bin/env python3
"""
Rune Performance Report Generator

Reads criterion JSON output and load test results, produces a Markdown report.

Usage:
    python3 benchmarks/report.py > benchmarks/results/report.md
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CRITERION_DIR = ROOT / "target" / "criterion"
LOAD_RESULTS = ROOT / "benchmarks" / "results" / "load.txt"


def get_git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, cwd=ROOT,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def get_hardware_info() -> str:
    try:
        result = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True, text=True,
        )
        cpu = result.stdout.strip()
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True, text=True,
        )
        mem_bytes = int(result.stdout.strip())
        mem_gb = mem_bytes // (1024 ** 3)
        return f"{cpu} / {mem_gb}GB"
    except Exception:
        try:
            # Fallback for Apple Silicon
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True,
            )
            return result.stdout.strip() or "Unknown"
        except Exception:
            return "Unknown"


def format_time(ns: float) -> str:
    """Format nanoseconds into human-readable time."""
    if ns < 1000:
        return f"{ns:.2f}ns"
    elif ns < 1_000_000:
        return f"{ns / 1000:.2f}us"
    elif ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f}ms"
    else:
        return f"{ns / 1_000_000_000:.2f}s"


def read_criterion_benchmarks() -> list[dict]:
    """Read criterion JSON estimates from target/criterion/."""
    benchmarks = []

    if not CRITERION_DIR.exists():
        return benchmarks

    def _try_read_estimates(path: Path, name: str):
        """Try to read estimates.json and append to benchmarks list."""
        estimates_path = path / "new" / "estimates.json"
        if not estimates_path.exists():
            return
        try:
            with open(estimates_path) as f:
                data = json.load(f)

            mean = data.get("mean", {})
            median = data.get("median", {})
            std_dev = data.get("std_dev", {})

            bench_info = {
                "name": name,
                "mean": mean.get("point_estimate", 0),
                "mean_lb": mean.get("confidence_interval", {}).get("lower_bound", 0),
                "mean_ub": mean.get("confidence_interval", {}).get("upper_bound", 0),
                "median": median.get("point_estimate", 0),
                "std_dev": std_dev.get("point_estimate", 0),
            }
            benchmarks.append(bench_info)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to parse {estimates_path}: {e}", file=sys.stderr)

    for group_dir in sorted(CRITERION_DIR.iterdir()):
        if not group_dir.is_dir() or group_dir.name == "report":
            continue

        for bench_dir in sorted(group_dir.iterdir()):
            if not bench_dir.is_dir() or bench_dir.name == "report":
                continue

            # Check if this directory itself has estimates (2-level: group/bench)
            if (bench_dir / "new" / "estimates.json").exists():
                _try_read_estimates(bench_dir, f"{group_dir.name}/{bench_dir.name}")
            else:
                # Check for 3-level nesting: group/bench/param (BenchmarkId)
                for param_dir in sorted(bench_dir.iterdir()):
                    if not param_dir.is_dir() or param_dir.name == "report":
                        continue
                    _try_read_estimates(
                        param_dir,
                        f"{group_dir.name}/{bench_dir.name}/{param_dir.name}",
                    )

    return benchmarks


def read_load_test_results() -> str:
    """Read raw load test output."""
    if LOAD_RESULTS.exists():
        return LOAD_RESULTS.read_text()
    return ""


def parse_load_test_results(raw: str) -> list[dict]:
    """Parse hey output sections into structured data."""
    results = []
    current_endpoint = None
    current_data = {}

    for line in raw.splitlines():
        line = line.strip()

        if line.startswith("---") and line.endswith("---"):
            # Save previous endpoint
            if current_endpoint and current_data:
                results.append({"endpoint": current_endpoint, **current_data})
            # Extract endpoint name
            name = line.strip("-").strip()
            # Map display names -> extract number prefix
            current_endpoint = name
            current_data = {}

        elif "Requests/sec:" in line:
            try:
                current_data["rps"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif "Average:" in line and "rps" not in line.lower():
            try:
                val = line.split(":")[1].strip().replace("secs", "").strip()
                current_data["avg_latency"] = float(val)
            except (ValueError, IndexError):
                pass
        elif "Fastest:" in line:
            try:
                val = line.split(":")[1].strip().replace("secs", "").strip()
                current_data["fastest"] = float(val)
            except (ValueError, IndexError):
                pass
        elif "Slowest:" in line:
            try:
                val = line.split(":")[1].strip().replace("secs", "").strip()
                current_data["slowest"] = float(val)
            except (ValueError, IndexError):
                pass
        elif "99%" in line:
            try:
                parts = line.split()
                # "99% in X secs"
                for j, p in enumerate(parts):
                    if p == "in":
                        current_data["p99"] = float(parts[j + 1])
                        break
            except (ValueError, IndexError):
                pass

    # Don't forget the last one
    if current_endpoint and current_data:
        results.append({"endpoint": current_endpoint, **current_data})

    return results


def generate_report():
    """Generate the full Markdown report."""
    commit = get_git_commit()
    hardware = get_hardware_info()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    benchmarks = read_criterion_benchmarks()
    load_raw = read_load_test_results()
    load_results = parse_load_test_results(load_raw)

    lines = []
    lines.append("# Rune Performance Report")
    lines.append("")
    lines.append(f"**Date**: {now}")
    lines.append(f"**Commit**: {commit}")
    lines.append(f"**Hardware**: {hardware}")
    lines.append("")

    # Micro-benchmarks
    lines.append("## Micro-benchmarks (criterion)")
    lines.append("")
    if benchmarks:
        lines.append("| Benchmark | Mean | Median | Std Dev | CI Lower | CI Upper |")
        lines.append("|-----------|------|--------|---------|----------|----------|")
        for b in benchmarks:
            lines.append(
                f"| {b['name']} "
                f"| {format_time(b['mean'])} "
                f"| {format_time(b['median'])} "
                f"| {format_time(b['std_dev'])} "
                f"| {format_time(b['mean_lb'])} "
                f"| {format_time(b['mean_ub'])} |"
            )
    else:
        lines.append("*No criterion results found. Run `cargo bench --bench core_benchmarks` first.*")
    lines.append("")

    # Load tests
    lines.append("## Load Tests (HTTP)")
    lines.append("")
    if load_results:
        lines.append("| Endpoint | RPS | Avg Latency | P99 Latency | Fastest | Slowest |")
        lines.append("|----------|-----|-------------|-------------|---------|---------|")
        for r in load_results:
            rps = f"{r.get('rps', 0):.1f}"
            avg = f"{r.get('avg_latency', 0) * 1000:.2f}ms" if r.get("avg_latency") else "N/A"
            p99 = f"{r.get('p99', 0) * 1000:.2f}ms" if r.get("p99") else "N/A"
            fastest = f"{r.get('fastest', 0) * 1000:.2f}ms" if r.get("fastest") else "N/A"
            slowest = f"{r.get('slowest', 0) * 1000:.2f}ms" if r.get("slowest") else "N/A"
            lines.append(f"| {r['endpoint']} | {rps} | {avg} | {p99} | {fastest} | {slowest} |")
    elif load_raw:
        lines.append("```")
        lines.append(load_raw.strip())
        lines.append("```")
    else:
        lines.append("*No load test results found. Run `./benchmarks/load_test.sh` first.*")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    if benchmarks:
        # Find slowest benchmark
        slowest = max(benchmarks, key=lambda b: b["mean"])
        fastest = min(benchmarks, key=lambda b: b["mean"])
        lines.append(f"- **Fastest operation**: {fastest['name']} ({format_time(fastest['mean'])})")
        lines.append(f"- **Slowest operation**: {slowest['name']} ({format_time(slowest['mean'])})")

        # Group by category
        categories = {}
        for b in benchmarks:
            cat = b["name"].split("/")[0]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(b)

        lines.append("")
        lines.append("### By Category")
        lines.append("")
        for cat, items in sorted(categories.items()):
            avg = sum(b["mean"] for b in items) / len(items)
            lines.append(f"- **{cat}**: {len(items)} benchmarks, avg {format_time(avg)}")
    else:
        lines.append("- Run benchmarks first to see summary analysis")

    lines.append("")

    print("\n".join(lines))


if __name__ == "__main__":
    generate_report()
