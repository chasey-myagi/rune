# Rune Performance Report

**Date**: 2026-03-23 15:40:43 UTC
**Commit**: beb3ccd
**Hardware**: Apple M4 / 16GB

## Micro-benchmarks (criterion)

| Benchmark | Mean | Median | Std Dev | CI Lower | CI Upper |
|-----------|------|--------|---------|----------|----------|
| dag/flow_execute_diamond | 23.43us | 23.35us | 693.75ns | 23.31us | 23.57us |
| dag/flow_execute_linear_3 | 22.64us | 22.56us | 716.73ns | 22.52us | 22.80us |
| dag/topological_layers | 1.64us | 1.64us | 26.28ns | 1.63us | 1.64us |
| dag/validate_10_steps | 874.05ns | 869.36ns | 22.45ns | 869.97ns | 878.70ns |
| invoker/local_invoke_once | 110.32ns | 109.60ns | 2.81ns | 109.79ns | 110.89ns |
| invoker/local_invoke_stream_10chunks | 7.34us | 7.29us | 209.31ns | 7.30us | 7.38us |
| rate_limit/check | 55.77ns | 55.55ns | 0.98ns | 55.59ns | 55.97ns |
| relay/list_100 | 4.70us | 4.68us | 142.72ns | 4.67us | 4.73us |
| relay/register | 1.51us | 1.50us | 32.03ns | 1.50us | 1.51us |
| relay/resolve/1 | 34.76ns | 34.77ns | 0.83ns | 34.60ns | 34.92ns |
| relay/resolve/10 | 35.70ns | 35.50ns | 1.41ns | 35.46ns | 36.01ns |
| relay/resolve/100 | 35.62ns | 35.61ns | 0.79ns | 35.47ns | 35.78ns |
| relay/resolve_with_labels | 948.28ns | 946.11ns | 24.26ns | 943.72ns | 953.12ns |
| schema/openapi_generate_10 | 41.80us | 41.74us | 890.69ns | 41.63us | 41.98us |
| schema/validate_input_cold | 6.31us | 6.30us | 116.50ns | 6.29us | 6.34us |
| schema/validate_input_invalid | 906.34ns | 903.21ns | 18.71ns | 903.05ns | 910.33ns |
| schema/validate_input_valid | 378.46ns | 377.18ns | 8.31ns | 377.00ns | 380.23ns |
| store/insert_log | 10.42us | 10.36us | 334.35ns | 10.36us | 10.49us |
| store/query_logs_50 | 24.35us | 24.18us | 866.00ns | 24.20us | 24.53us |
| store/stats_enhanced | 73.98us | 73.73us | 1.56us | 73.68us | 74.29us |

## Load Tests (HTTP)

*No load test results found. Run `./benchmarks/load_test.sh` first.*

## Summary

- **Fastest operation**: relay/resolve/1 (34.76ns)
- **Slowest operation**: store/stats_enhanced (73.98us)

### By Category

- **dag**: 4 benchmarks, avg 12.15us
- **invoker**: 2 benchmarks, avg 3.72us
- **rate_limit**: 1 benchmarks, avg 55.77ns
- **relay**: 6 benchmarks, avg 1.21us
- **schema**: 4 benchmarks, avg 12.35us
- **store**: 3 benchmarks, avg 36.25us

