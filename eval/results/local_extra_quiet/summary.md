| cell | method | repeats | workload_s_mean | workload_s_sd | ratio_to_no_cache | ratio_ci95 | timing | empty_queries | ratio_rows_only | exact_hits | contained_hits | misses | coverage_s_mean | scan_mb | coverage_mb | records_read_m | cache_entries | cache_peak_kb | cache_heap_peak_kb | evictions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| zipf_contained_sorted | NO_CACHE | 1 | 5.581 | None | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 116.9 | 0.0 | 1.63 | 0 | 0.0 | 0.0 | 0 |
| zipf_contained_sorted | PCC_INLINE | 1 | 5.299 | None | 0.95 | (0.884, 1.015) | not separated | 0 | 0.95 | 11 | 7 | 32 | 0.0 | 87.7 | 0.0 | 1.63 | 32 | 6.0 | 18.5 | 0 |
| zipf_skewdata | NO_CACHE | 1 | 6.369 | None | None | (None, None) | baseline | 15 | None | 0 | 0 | 0 | 0.0 | 1421.5 | 0.0 | 26.95 | 0 | 0.0 | 0.0 | 0 |
| zipf_skewdata | PCC_INLINE | 1 | 5.558 | None | 0.873 | (0.744, 0.99) | not separated | 15 | 1.11 | 11 | 7 | 32 | 0.0 | 950.6 | 0.0 | 18.02 | 38 | 52.1 | 128.1 | 0 |
| share_repeat0_skewdata | NO_CACHE | 3 | 8.291 | 2.026 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 1424.5 | 0.0 | 27.01 | 0 | 0.0 | 0.0 | 0 |
| share_repeat0_skewdata | NAIVE_CACHE | 3 | 7.25 | 1.308 | 0.874 | (0.765, 0.976) | not separated | 0 | 0.874 | 5 | 0 | 45 | 0.0 | 1282.0 | 0.0 | 24.31 | 45 | 2.9 | 13.3 | 0 |
| share_repeat0_skewdata | EXACT_FILE | 3 | 8.393 | 1.717 | 1.012 | (0.907, 1.101) | not separated | 0 | 1.012 | 5 | 0 | 45 | 0.0 | 1293.1 | 0.0 | 24.52 | 45 | 161.1 | 387.0 | 0 |
| share_repeat0_skewdata | PCC | 3 | 11.698 | 1.639 | 1.411 | (1.246, 1.568) | separated, higher | 0 | 1.411 | 5 | 16 | 29 | 5.17 | 943.2 | 943.2 | 35.77 | 34 | 77.9 | 188.4 | 0 |
| share_repeat0_skewdata | PCC_INLINE | 3 | 8.092 | 1.625 | 0.976 | (0.873, 1.076) | not separated | 0 | 0.976 | 5 | 16 | 29 | 0.0 | 943.2 | 0.0 | 17.89 | 34 | 77.9 | 188.4 | 0 |
| share_repeat30_skewdata | NO_CACHE | 3 | 7.604 | 0.87 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 1424.5 | 0.0 | 27.01 | 0 | 0.0 | 0.0 | 0 |
| share_repeat30_skewdata | NAIVE_CACHE | 3 | 5.86 | 0.397 | 0.771 | (0.655, 0.866) | separated, lower | 0 | 0.771 | 11 | 0 | 39 | 0.0 | 1111.1 | 0.0 | 21.07 | 39 | 2.5 | 11.5 | 0 |
| share_repeat30_skewdata | EXACT_FILE | 3 | 8.521 | 1.038 | 1.121 | (1.046, 1.203) | not separated | 0 | 1.121 | 11 | 0 | 39 | 0.0 | 1261.0 | 0.0 | 23.91 | 39 | 155.7 | 373.3 | 0 |
| share_repeat30_skewdata | PCC | 3 | 11.61 | 1.169 | 1.527 | (1.441, 1.611) | separated, higher | 0 | 1.527 | 5 | 5 | 40 | 5.234 | 1179.2 | 1179.2 | 44.72 | 23 | 46.4 | 112.7 | 0 |
| share_repeat30_skewdata | PCC_INLINE | 3 | 7.342 | 0.414 | 0.965 | (0.91, 1.018) | not separated | 0 | 0.965 | 5 | 5 | 40 | 0.0 | 1179.2 | 0.0 | 22.36 | 23 | 46.4 | 112.7 | 0 |
| share_repeat60_skewdata | NO_CACHE | 3 | 7.014 | 0.484 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 1424.5 | 0.0 | 27.01 | 0 | 0.0 | 0.0 | 0 |
| share_repeat60_skewdata | NAIVE_CACHE | 3 | 3.809 | 0.534 | 0.543 | (0.374, 0.695) | separated, lower | 0 | 0.543 | 28 | 0 | 22 | 0.0 | 626.8 | 0.0 | 11.88 | 22 | 1.4 | 6.5 | 0 |
| share_repeat60_skewdata | EXACT_FILE | 3 | 7.507 | 0.853 | 1.07 | (0.98, 1.169) | not separated | 0 | 1.07 | 28 | 0 | 22 | 0.0 | 896.6 | 0.0 | 17.0 | 22 | 75.4 | 181.2 | 0 |
| share_repeat60_skewdata | PCC | 3 | 11.014 | 1.42 | 1.57 | (1.459, 1.669) | separated, higher | 0 | 1.57 | 20 | 0 | 30 | 4.838 | 972.1 | 972.1 | 36.87 | 15 | 25.8 | 63.0 | 0 |
| share_repeat60_skewdata | PCC_INLINE | 3 | 6.974 | 0.718 | 0.994 | (0.931, 1.056) | not separated | 0 | 0.994 | 20 | 0 | 30 | 0.0 | 972.1 | 0.0 | 18.43 | 15 | 25.8 | 63.0 | 0 |
| budget10_volume_sorted | PCC_INLINE | 1 | 5.939 | None | 1.064 | (1.024, 1.105) | not separated | 0 | 1.064 | 0 | 2 | 48 | 0.0 | 113.7 | 0.0 | 1.63 | 2 | 0.6 | 1.8 | 46 |
| budget10_coverage_sorted | PCC_INLINE | 1 | 6.672 | None | 1.195 | (1.118, 1.271) | separated, higher | 0 | 1.195 | 5 | 2 | 43 | 0.0 | 105.5 | 0.0 | 1.63 | 3 | 0.6 | 1.8 | 40 |
| budget10_volume_skewqty | PCC_INLINE | 1 | 7.519 | None | 1.181 | (1.071, 1.278) | separated, higher | 15 | 1.323 | 2 | 2 | 46 | 0.0 | 1317.8 | 0.0 | 24.98 | 3 | 5.2 | 12.7 | 43 |
| budget10_coverage_skewqty | PCC_INLINE | 1 | 6.894 | None | 1.082 | (0.918, 1.244) | not separated | 15 | 1.391 | 11 | 0 | 39 | 0.0 | 1108.1 | 0.0 | 21.01 | 15 | 5.2 | 14.0 | 23 |

| cell | first | second | ratio | 95% interval | reading |
|---|---|---|---|---|---|
| share_repeat0_skewdata | PCC_INLINE | PCC | 0.692 | (0.667, 0.719) | separated, lower |
| share_repeat0_skewdata | PCC_INLINE | EXACT_FILE | 0.964 | (0.908, 1.017) | not separated |
| share_repeat0_skewdata | PCC | EXACT_FILE | 1.394 | (1.307, 1.476) | separated, higher |
| share_repeat0_skewdata | NAIVE_CACHE | PCC_INLINE | 0.896 | (0.824, 0.973) | not separated |
| share_repeat30_skewdata | PCC_INLINE | PCC | 0.632 | (0.616, 0.649) | separated, lower |
| share_repeat30_skewdata | PCC_INLINE | EXACT_FILE | 0.862 | (0.807, 0.916) | separated, lower |
| share_repeat30_skewdata | PCC | EXACT_FILE | 1.362 | (1.28, 1.445) | separated, higher |
| share_repeat30_skewdata | NAIVE_CACHE | PCC_INLINE | 0.798 | (0.696, 0.893) | separated, lower |
| share_repeat60_skewdata | PCC_INLINE | PCC | 0.633 | (0.613, 0.655) | separated, lower |
| share_repeat60_skewdata | PCC_INLINE | EXACT_FILE | 0.929 | (0.873, 0.982) | not separated |
| share_repeat60_skewdata | PCC | EXACT_FILE | 1.467 | (1.358, 1.572) | separated, higher |
| share_repeat60_skewdata | NAIVE_CACHE | PCC_INLINE | 0.546 | (0.398, 0.675) | separated, lower |
| budget10_coverage_sorted | coverage | volume | 1.123 | (1.045, 1.197) | not separated |
| budget10_coverage_skewqty | coverage | volume | 0.917 | (0.817, 1.008) | not separated |

Problems: {'mismatches': 0, 'failed_runs': 0, 'missing_runs': 0, 'short_runs': 0, 'hit_disagreement': 0, 'identity_mismatch': 0, 'skipped_runs': 0}
Problem details: {}
A/A control: []
Identity: {'git_sha': '4745e78f8255874e627569aebda8403fe7d9a51a', 'jar_sha256': 'd3adf86432f4415f79cee819e65d25e7bc4b508c13b52ca585c1edc99ab12645', 'plan_sha256': '7fac67f29bff8b66a816411ad7057e2aabf15ffcb01d7ae22cd9ba2bc5bdcc1f'}
