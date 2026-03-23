# Rune Performance Report

**Date**: 2026-03-23 15:13:58 UTC
**Commit**: 0697ef5
**Hardware**: Apple M4 / 16GB

## Micro-benchmarks (criterion)

| Benchmark | Mean | Median | Std Dev | CI Lower | CI Upper |
|-----------|------|--------|---------|----------|----------|
| dag/flow_execute_diamond | 23.61us | 23.31us | 1.37us | 23.37us | 23.90us |
| dag/flow_execute_linear_3 | 22.68us | 22.60us | 652.38ns | 22.56us | 22.81us |
| dag/topological_layers | 1.63us | 1.63us | 29.54ns | 1.63us | 1.64us |
| dag/validate_10_steps | 854.94ns | 853.46ns | 16.46ns | 851.84ns | 858.27ns |
| invoker/local_invoke_once | 111.16ns | 111.15ns | 1.98ns | 110.77ns | 111.55ns |
| invoker/local_invoke_stream_10chunks | 7.22us | 7.21us | 139.44ns | 7.19us | 7.24us |
| rate_limit/check | 56.36ns | 55.88ns | 1.91ns | 56.01ns | 56.76ns |
| relay/list_100 | 4.67us | 4.68us | 95.53ns | 4.66us | 4.69us |
| relay/register | 1.49us | 1.48us | 27.48ns | 1.48us | 1.49us |
| relay/resolve/1 | 34.90ns | 34.84ns | 1.07ns | 34.70ns | 35.12ns |
| relay/resolve/10 | 35.48ns | 35.46ns | 0.90ns | 35.30ns | 35.66ns |
| relay/resolve/100 | 35.79ns | 35.92ns | 0.91ns | 35.61ns | 35.97ns |
| relay/resolve_with_labels | 934.19ns | 933.57ns | 23.07ns | 929.72ns | 938.74ns |
| schema/openapi_generate_10 | 42.02us | 41.49us | 4.33us | 41.49us | 42.95us |
| schema/validate_input_invalid | 6.88us | 6.88us | 121.50ns | 6.86us | 6.91us |
| schema/validate_input_valid | 6.37us | 6.30us | 273.75ns | 6.32us | 6.43us |
| store/insert_log | 10.89us | 10.68us | 1.15us | 10.70us | 11.14us |
| store/query_logs_50 | 25.04us | 24.53us | 2.79us | 24.63us | 25.68us |
| store/stats_enhanced | 115.17us | 114.29us | 4.33us | 114.38us | 116.07us |

## Load Tests (HTTP)

*No load test results found. Run `./benchmarks/load_test.sh` first.*

## Summary

- **Fastest operation**: relay/resolve/1 (34.90ns)
- **Slowest operation**: store/stats_enhanced (115.17us)

### By Category

- **dag**: 4 benchmarks, avg 12.19us
- **invoker**: 2 benchmarks, avg 3.66us
- **rate_limit**: 1 benchmarks, avg 56.36ns
- **relay**: 6 benchmarks, avg 1.20us
- **schema**: 3 benchmarks, avg 18.42us
- **store**: 3 benchmarks, avg 50.37us

