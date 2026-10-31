| cell | method | repeats | workload_s_mean | workload_s_sd | ratio_to_no_cache | ratio_ci95 | timing | empty_queries | ratio_rows_only | exact_hits | contained_hits | misses | coverage_s_mean | scan_mb | coverage_mb | records_read_m | cache_entries | cache_peak_kb | cache_heap_peak_kb | evictions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| heldout_mix_sorted | NO_CACHE | 3 | 5.686 | 0.064 | None | (None, None) | baseline | 4 | None | 0 | 0 | 0 | 0.0 | 124.6 | 0.0 | 2.44 | 0 | 0.0 | 0.0 | 0 |
| heldout_mix_sorted | NAIVE_CACHE | 3 | 4.759 | 0.215 | 0.837 | (0.723, 0.943) | separated, lower | 4 | 0.821 | 10 | 0 | 40 | 0.0 | 97.1 | 0.0 | 1.81 | 40 | 2.7 | 12.0 | 0 |
| heldout_mix_sorted | EXACT_FILE | 3 | 5.778 | 0.036 | 1.016 | (0.958, 1.07) | not separated | 4 | 1.021 | 10 | 0 | 40 | 0.0 | 108.5 | 0.0 | 2.44 | 40 | 7.4 | 22.9 | 0 |
| heldout_mix_sorted | PCC | 3 | 8.3 | 0.175 | 1.46 | (1.347, 1.568) | separated, higher | 4 | 1.458 | 10 | 9 | 31 | 3.653 | 94.0 | 78.9 | 4.88 | 34 | 6.1 | 19.2 | 0 |
| heldout_mix_sorted | PCC_INLINE | 3 | 5.423 | 0.11 | 0.954 | (0.891, 1.013) | not separated | 4 | 0.951 | 10 | 9 | 31 | 0.0 | 94.0 | 0.0 | 2.44 | 34 | 6.1 | 19.2 | 0 |

| cell | first | second | ratio | 95% interval | reading |
|---|---|---|---|---|---|
| heldout_mix_sorted | PCC_INLINE | PCC | 0.653 | (0.635, 0.674) | separated, lower |
| heldout_mix_sorted | PCC_INLINE | EXACT_FILE | 0.939 | (0.894, 0.979) | not separated |
| heldout_mix_sorted | PCC | EXACT_FILE | 1.437 | (1.348, 1.518) | separated, higher |
| heldout_mix_sorted | NAIVE_CACHE | PCC_INLINE | 0.877 | (0.782, 0.963) | not separated |

Problems: {'mismatches': 0, 'failed_runs': 0, 'missing_runs': 0, 'short_runs': 0, 'hit_disagreement': 0, 'identity_mismatch': 0, 'skipped_runs': 0}
Problem details: {}
A/A control: []
Identity: {'git_sha': 'd7e8559af58a740b86d53b33f90511410a2c6f83', 'jar_sha256': '3afac74cc61c3a255fdb9bee2710f855e9eab29667210864c3f7238a28127e92', 'plan_sha256': '3ae4d0c708d51353afbff15fc57d550267b7c6d984760c2fb923f20c9786e8df'}
