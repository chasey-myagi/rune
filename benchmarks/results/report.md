# Rune Performance Report

**Date**: 2026-03-23 15:09:39 UTC
**Commit**: 3b8cc91
**Hardware**: Apple M4 / 16GB

## Micro-benchmarks (criterion)

| Benchmark | Mean | Median | Std Dev | CI Lower | CI Upper |
|-----------|------|--------|---------|----------|----------|
| dag/flow_execute_diamond | 21.76us | 21.76us | 419.69ns | 21.47us | 22.06us |
| dag/flow_execute_linear_3 | 20.82us | 20.82us | 547.85ns | 20.43us | 21.20us |
| dag/topological_layers | 1.73us | 1.73us | 10.08ns | 1.72us | 1.73us |
| dag/validate_10_steps | 957.38ns | 957.38ns | 23.89ns | 940.48ns | 974.27ns |
| invoker/local_invoke_once | 118.91ns | 118.91ns | 2.88ns | 116.88ns | 120.95ns |
| invoker/local_invoke_stream_10chunks | 6.78us | 6.78us | 128.21ns | 6.69us | 6.87us |
| rate_limit/check | 59.64ns | 59.64ns | 1.59ns | 58.52ns | 60.76ns |
| relay/list_100 | 4.88us | 4.88us | 102.84ns | 4.81us | 4.95us |
| relay/register | 1.62us | 1.62us | 47.34ns | 1.58us | 1.65us |
| relay/resolve/1 | 35.98ns | 35.98ns | 0.70ns | 35.48ns | 36.47ns |
| relay/resolve/10 | 36.70ns | 36.70ns | 0.58ns | 36.29ns | 37.11ns |
| relay/resolve/100 | 37.34ns | 37.34ns | 1.33ns | 36.40ns | 38.29ns |
| relay/resolve_with_labels | 1.03us | 1.03us | 27.34ns | 1.01us | 1.05us |
| schema/openapi_generate_10 | 43.13us | 43.13us | 1.35us | 42.17us | 44.08us |
| schema/validate_input_invalid | 7.00us | 7.00us | 121.48ns | 6.91us | 7.09us |
| schema/validate_input_valid | 6.71us | 6.71us | 248.78ns | 6.53us | 6.88us |
| store/insert_log | 9.55us | 9.55us | 203.29ns | 9.40us | 9.69us |
| store/query_logs_50 | 23.21us | 23.21us | 665.40ns | 22.74us | 23.68us |
| store/stats_enhanced | 117.00us | 117.00us | 4.49us | 113.83us | 120.17us |

## Load Tests (HTTP)

*No load test results found. Run `./benchmarks/load_test.sh` first.*

## Summary

- **Fastest operation**: relay/resolve/1 (35.98ns)
- **Slowest operation**: store/stats_enhanced (117.00us)

### By Category

- **dag**: 4 benchmarks, avg 11.32us
- **invoker**: 2 benchmarks, avg 3.45us
- **rate_limit**: 1 benchmarks, avg 59.64ns
- **relay**: 6 benchmarks, avg 1.27us
- **schema**: 3 benchmarks, avg 18.94us
- **store**: 3 benchmarks, avg 49.92us

