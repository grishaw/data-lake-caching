| cell | method | repeats | workload_s_mean | workload_s_sd | ratio_to_no_cache | ratio_ci95 | timing | empty_queries | ratio_rows_only | exact_hits | contained_hits | misses | coverage_s_mean | scan_mb | coverage_mb | records_read_m | cache_entries | cache_peak_kb | cache_heap_peak_kb | evictions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| table3_100 | NO_CACHE | 3 | 12.486 | 0.294 | None | (None, None) | baseline | 8 | None | 0 | 0 | 0 | 0.0 | 1134.3 | 0.0 | 33.95 | 0 | 0.0 | 0.0 | 0 |
| table3_100 | NAIVE_CACHE | 3 | 12.333 | 0.135 | 0.988 | (0.972, 1.004) | not separated | 8 | 0.984 | 0 | 0 | 100 | 0.0 | 1134.3 | 0.0 | 33.95 | 100 | 6.1 | 29.0 | 0 |
| table3_100 | PCC | 3 | 15.53 | 0.633 | 1.244 | (1.169, 1.322) | separated, higher | 8 | 1.278 | 0 | 83 | 17 | 6.806 | 620.2 | 489.5 | 41.63 | 61 | 43.1 | 110.9 | 0 |

| cell | first | second | ratio | 95% interval | reading |
|---|---|---|---|---|---|

Table 3 table3_100 NO_CACHE 10 queries: paper timer 1.7 s, all costs 1.7 s
Table 3 table3_100 NO_CACHE 50 queries: paper timer 6.8 s, all costs 6.8 s
Table 3 table3_100 NO_CACHE 100 queries: paper timer 12.5 s, all costs 12.5 s
Table 3 table3_100 NAIVE_CACHE 10 queries: paper timer 1.7 s, all costs 1.7 s
Table 3 table3_100 NAIVE_CACHE 50 queries: paper timer 6.8 s, all costs 6.8 s
Table 3 table3_100 NAIVE_CACHE 100 queries: paper timer 12.3 s, all costs 12.3 s
Table 3 table3_100 PCC 10 queries: paper timer 1.5 s, all costs 2.8 s
Table 3 table3_100 PCC 50 queries: paper timer 4.8 s, all costs 8.6 s
Table 3 table3_100 PCC 100 queries: paper timer 8.7 s, all costs 15.5 s
Problems: {'mismatches': 0, 'failed_runs': 0, 'missing_runs': 0, 'short_runs': 0, 'hit_disagreement': 0, 'identity_mismatch': 0, 'skipped_runs': 0}
Problem details: {}
A/A control: []
Identity: {'git_sha': '46f40ef05172582478c56d3ef04ce3da0faac033', 'jar_sha256': '483c574a10441263024b4825a1a24b73cf676697b6cdc3f1a9338825f0c92e2c', 'plan_sha256': '3d17f2a417f00af860b391c638eb51a3fc88653bafc0f5947e8b3086e769aa9f'}
