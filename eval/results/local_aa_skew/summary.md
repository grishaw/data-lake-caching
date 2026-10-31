| cell | method | repeats | workload_s_mean | workload_s_sd | ratio_to_no_cache | ratio_ci95 | timing | empty_queries | ratio_rows_only | exact_hits | contained_hits | misses | coverage_s_mean | scan_mb | coverage_mb | records_read_m | cache_entries | cache_peak_kb | cache_heap_peak_kb | evictions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aa_skewdata | NO_CACHE | 3 | 6.755 | 0.223 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 1424.5 | 0.0 | 27.01 | 0 | 0.0 | 0.0 | 0 |
| aa_skewdata | NO_CACHE@2 | 3 | 6.623 | 0.113 | 0.98 | (0.962, 0.999) | not separated | 0 | 0.98 | 0 | 0 | 0 | 0.0 | 1424.5 | 0.0 | 27.01 | 0 | 0.0 | 0.0 | 0 |

| cell | first | second | ratio | 95% interval | reading |
|---|---|---|---|---|---|

Problems: {'mismatches': 0, 'failed_runs': 0, 'missing_runs': 0, 'short_runs': 0, 'hit_disagreement': 0, 'identity_mismatch': 0, 'skipped_runs': 0}
Problem details: {}
A/A control: [{'cell': 'aa_skewdata', 'ratio': 0.98, 'ci95': (0.962, 0.999), 'timing': 'not separated'}]
Identity: {'git_sha': '4745e78f8255874e627569aebda8403fe7d9a51a', 'jar_sha256': 'd3adf86432f4415f79cee819e65d25e7bc4b508c13b52ca585c1edc99ab12645', 'plan_sha256': '316a606e45c4dfe16b63ae64dbcb103cbecb0cbbcedaeb1ef0b439fc5e3fa6ae'}
