| cell | method | repeats | workload_s_mean | workload_s_sd | ratio_to_no_cache | ratio_ci95 | timing | empty_queries | ratio_rows_only | exact_hits | contained_hits | misses | coverage_s_mean | scan_mb | coverage_mb | records_read_m | cache_entries | cache_peak_kb | cache_heap_peak_kb | evictions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| paper_generator_seed1 | NO_CACHE | 3 | 6.029 | 0.105 | None | (None, None) | baseline | 1 | None | 0 | 0 | 0 | 0.0 | 567.2 | 0.0 | 16.97 | 0 | 0.0 | 0.0 | 0 |
| paper_generator_seed1 | NAIVE_CACHE | 3 | 5.958 | 0.039 | 0.988 | (0.97, 1.008) | not separated | 1 | 0.986 | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 3.2 | 14.7 | 0 |
| paper_generator_seed1 | EXACT_FILE | 3 | 6.593 | 0.114 | 1.094 | (1.07, 1.116) | separated, higher | 1 | 1.094 | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 35.0 | 90.3 | 0 |
| paper_generator_seed1 | PCC | 3 | 7.693 | 0.057 | 1.276 | (1.18, 1.374) | separated, higher | 1 | 1.279 | 0 | 42 | 8 | 3.399 | 315.2 | 249.0 | 20.94 | 36 | 23.0 | 59.9 | 0 |
| paper_generator_seed1 | PCC_INLINE | 3 | 5.115 | 0.066 | 0.848 | (0.791, 0.903) | separated, lower | 1 | 0.853 | 0 | 42 | 8 | 0.0 | 315.2 | 0.0 | 10.47 | 36 | 23.0 | 59.9 | 0 |
| paper_generator_seed1 | NO_CACHE@2 | 3 | 6.034 | 0.027 | 1.001 | (0.981, 1.02) | not separated | 1 | 0.999 | 0 | 0 | 0 | 0.0 | 567.2 | 0.0 | 16.97 | 0 | 0.0 | 0.0 | 0 |
| official_q6_sorted | NO_CACHE | 3 | 8.246 | 0.049 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 651.3 | 0.0 | 46.79 | 0 | 0.0 | 0.0 | 0 |
| official_q6_sorted | NAIVE_CACHE | 3 | 6.434 | 0.071 | 0.78 | (0.67, 0.872) | separated, lower | 0 | 0.78 | 14 | 0 | 36 | 0.0 | 468.5 | 0.0 | 33.65 | 36 | 2.6 | 11.3 | 0 |
| official_q6_sorted | EXACT_FILE | 3 | 9.896 | 0.431 | 1.2 | (1.142, 1.263) | separated, higher | 0 | 1.2 | 14 | 0 | 36 | 0.0 | 632.0 | 0.0 | 46.79 | 36 | 63.6 | 156.5 | 0 |
| official_q6_sorted | PCC | 3 | 15.037 | 0.092 | 1.824 | (1.743, 1.905) | separated, higher | 0 | 1.824 | 12 | 8 | 30 | 6.931 | 623.8 | 374.1 | 93.58 | 30 | 53.0 | 130.5 | 0 |
| official_q6_sorted | PCC_INLINE | 3 | 9.641 | 0.294 | 1.169 | (1.111, 1.235) | separated, higher | 0 | 1.169 | 12 | 8 | 30 | 0.0 | 623.8 | 0.0 | 46.79 | 30 | 53.0 | 130.5 | 0 |
| zipf_contained_sorted | NO_CACHE | 3 | 5.564 | 0.028 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 116.9 | 0.0 | 1.63 | 0 | 0.0 | 0.0 | 0 |
| zipf_contained_sorted | NAIVE_CACHE | 3 | 4.544 | 0.046 | 0.817 | (0.7, 0.918) | separated, lower | 0 | 0.817 | 11 | 0 | 39 | 0.0 | 92.8 | 0.0 | 1.41 | 39 | 2.6 | 11.8 | 0 |
| zipf_contained_sorted | EXACT_FILE | 3 | 5.613 | 0.193 | 1.009 | (0.946, 1.071) | not separated | 0 | 1.009 | 11 | 0 | 39 | 0.0 | 99.1 | 0.0 | 1.63 | 39 | 7.2 | 22.5 | 0 |
| zipf_contained_sorted | PCC | 3 | 8.014 | 0.045 | 1.44 | (1.323, 1.547) | separated, higher | 0 | 1.44 | 11 | 7 | 32 | 3.509 | 87.7 | 77.3 | 3.25 | 32 | 6.0 | 18.5 | 0 |
| zipf_contained_sorted | PCC_INLINE | 3 | 5.278 | 0.024 | 0.949 | (0.874, 1.019) | not separated | 0 | 0.949 | 11 | 7 | 32 | 0.0 | 87.7 | 0.0 | 1.63 | 32 | 6.0 | 18.5 | 0 |
| zipf_contained_sorted | NO_CACHE@2 | 3 | 5.532 | 0.041 | 0.994 | (0.975, 1.013) | not separated | 0 | 0.994 | 0 | 0 | 0 | 0.0 | 116.9 | 0.0 | 1.63 | 0 | 0.0 | 0.0 | 0 |
| share_repeat0_sorted | NO_CACHE | 3 | 5.654 | 0.012 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 130.2 | 0.0 | 2.58 | 0 | 0.0 | 0.0 | 0 |
| share_repeat0_sorted | EXACT_FILE | 3 | 5.943 | 0.049 | 1.051 | (1.002, 1.094) | not separated | 0 | 1.051 | 5 | 0 | 45 | 0.0 | 122.1 | 0.0 | 2.58 | 45 | 10.0 | 29.9 | 0 |
| share_repeat0_sorted | PCC_INLINE | 3 | 4.911 | 0.003 | 0.869 | (0.8, 0.932) | separated, lower | 0 | 0.869 | 1 | 31 | 18 | 0.0 | 78.7 | 0.0 | 2.58 | 19 | 4.2 | 12.6 | 0 |
| share_repeat30_sorted | NO_CACHE | 3 | 5.563 | 0.034 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 122.9 | 0.0 | 2.15 | 0 | 0.0 | 0.0 | 0 |
| share_repeat30_sorted | EXACT_FILE | 3 | 5.693 | 0.163 | 1.023 | (0.964, 1.079) | not separated | 0 | 1.023 | 11 | 0 | 39 | 0.0 | 105.2 | 0.0 | 2.15 | 39 | 7.5 | 23.2 | 0 |
| share_repeat30_sorted | PCC_INLINE | 3 | 5.205 | 0.044 | 0.936 | (0.864, 1.006) | not separated | 0 | 0.936 | 10 | 13 | 27 | 0.0 | 85.8 | 0.0 | 2.15 | 29 | 5.7 | 17.6 | 0 |
| share_repeat60_sorted | NO_CACHE | 3 | 5.478 | 0.101 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 119.8 | 0.0 | 1.87 | 0 | 0.0 | 0.0 | 0 |
| share_repeat60_sorted | EXACT_FILE | 3 | 4.75 | 0.042 | 0.867 | (0.793, 0.946) | separated, lower | 0 | 0.867 | 28 | 0 | 22 | 0.0 | 74.6 | 0.0 | 1.87 | 22 | 3.8 | 12.1 | 0 |
| share_repeat60_sorted | PCC_INLINE | 3 | 4.751 | 0.069 | 0.867 | (0.789, 0.945) | separated, lower | 0 | 0.867 | 28 | 0 | 22 | 0.0 | 74.6 | 0.0 | 1.87 | 22 | 3.8 | 12.1 | 0 |
| paper_generator_seed2 | NO_CACHE | 3 | 5.972 | 0.029 | None | (None, None) | baseline | 3 | None | 0 | 0 | 0 | 0.0 | 567.2 | 0.0 | 16.97 | 0 | 0.0 | 0.0 | 0 |
| paper_generator_seed2 | NAIVE_CACHE | 3 | 5.937 | 0.031 | 0.994 | (0.98, 1.008) | not separated | 3 | 0.994 | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 3.2 | 14.7 | 0 |
| paper_generator_seed2 | EXACT_FILE | 3 | 6.561 | 0.104 | 1.099 | (1.073, 1.123) | separated, higher | 3 | 1.108 | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 39.8 | 101.7 | 0 |
| paper_generator_seed2 | PCC | 3 | 8.049 | 0.023 | 1.348 | (1.256, 1.445) | separated, higher | 3 | 1.359 | 0 | 34 | 16 | 3.598 | 355.3 | 282.4 | 23.17 | 39 | 29.7 | 76.0 | 0 |
| paper_generator_seed2 | PCC_INLINE | 3 | 5.39 | 0.056 | 0.903 | (0.847, 0.957) | not separated | 3 | 0.915 | 0 | 34 | 16 | 0.0 | 355.3 | 0.0 | 11.59 | 39 | 29.7 | 76.0 | 0 |
| paper_generator_seed3 | NO_CACHE | 3 | 5.993 | 0.062 | None | (None, None) | baseline | 1 | None | 0 | 0 | 0 | 0.0 | 567.2 | 0.0 | 16.97 | 0 | 0.0 | 0.0 | 0 |
| paper_generator_seed3 | NAIVE_CACHE | 3 | 6.01 | 0.058 | 1.003 | (0.979, 1.025) | not separated | 1 | 1.001 | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 3.2 | 14.7 | 0 |
| paper_generator_seed3 | EXACT_FILE | 3 | 6.651 | 0.1 | 1.11 | (1.085, 1.134) | separated, higher | 1 | 1.109 | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 38.1 | 97.7 | 0 |
| paper_generator_seed3 | PCC | 3 | 8.078 | 0.085 | 1.348 | (1.248, 1.455) | separated, higher | 1 | 1.355 | 0 | 32 | 18 | 3.591 | 358.2 | 285.3 | 23.18 | 41 | 29.8 | 76.5 | 0 |
| paper_generator_seed3 | PCC_INLINE | 3 | 5.447 | 0.058 | 0.909 | (0.851, 0.966) | not separated | 1 | 0.914 | 0 | 32 | 18 | 0.0 | 358.2 | 0.0 | 11.59 | 41 | 29.8 | 76.5 | 0 |
| paper_rounded_random | NO_CACHE | 3 | 5.853 | 0.04 | None | (None, None) | baseline | 1 | None | 0 | 0 | 0 | 0.0 | 567.2 | 0.0 | 16.97 | 0 | 0.0 | 0.0 | 0 |
| paper_rounded_random | NAIVE_CACHE | 3 | 4.361 | 0.044 | 0.745 | (0.616, 0.858) | separated, lower | 1 | 0.74 | 16 | 0 | 34 | 0.0 | 385.7 | 0.0 | 11.54 | 34 | 2.1 | 10.0 | 0 |
| paper_rounded_random | EXACT_FILE | 3 | 5.893 | 0.082 | 1.007 | (0.953, 1.059) | not separated | 1 | 1.007 | 16 | 0 | 34 | 0.0 | 465.5 | 0.0 | 14.38 | 34 | 28.6 | 72.8 | 0 |
| paper_rounded_random | PCC | 3 | 7.154 | 0.055 | 1.222 | (1.148, 1.298) | separated, higher | 1 | 1.233 | 11 | 36 | 3 | 3.147 | 281.7 | 220.7 | 19.54 | 22 | 17.8 | 45.4 | 0 |
| paper_rounded_random | PCC_INLINE | 3 | 4.814 | 0.026 | 0.823 | (0.781, 0.868) | separated, lower | 1 | 0.828 | 11 | 36 | 3 | 0.0 | 281.7 | 0.0 | 9.77 | 22 | 17.8 | 45.4 | 0 |
| paper_test_ranges_random | NO_CACHE | 3 | 6.015 | 0.088 | None | (None, None) | baseline | 50 | None | 0 | 0 | 0 | 0.0 | 567.2 | 0.0 | 16.97 | 0 | 0.0 | 0.0 | 0 |
| paper_test_ranges_random | NAIVE_CACHE | 3 | 6.015 | 0.018 | 1.0 | (0.983, 1.017) | not separated | 50 | None | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 2.5 | 13.3 | 0 |
| paper_test_ranges_random | EXACT_FILE | 3 | 5.971 | 0.061 | 0.993 | (0.97, 1.014) | not separated | 50 | None | 0 | 0 | 50 | 0.0 | 567.2 | 0.0 | 16.97 | 50 | 2.1 | 11.0 | 0 |
| paper_test_ranges_random | PCC | 3 | 1.825 | 0.018 | 0.303 | (0.104, 0.52) | separated, lower | 50 | None | 0 | 43 | 7 | 0.786 | 79.4 | 64.3 | 4.75 | 7 | 0.3 | 1.6 | 0 |
| paper_test_ranges_random | PCC_INLINE | 3 | 1.102 | 0.029 | 0.183 | (0.074, 0.311) | separated, lower | 50 | None | 0 | 43 | 7 | 0.0 | 79.4 | 0.0 | 2.38 | 7 | 0.3 | 1.6 | 0 |
| zipf_sorted | NO_CACHE | 3 | 5.51 | 0.128 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 120.8 | 0.0 | 1.93 | 0 | 0.0 | 0.0 | 0 |
| zipf_sorted | NAIVE_CACHE | 3 | 3.393 | 0.06 | 0.616 | (0.469, 0.749) | separated, lower | 0 | 0.616 | 23 | 0 | 27 | 0.0 | 64.7 | 0.0 | 1.1 | 27 | 1.8 | 8.2 | 0 |
| zipf_sorted | EXACT_FILE | 3 | 4.986 | 0.016 | 0.905 | (0.832, 0.972) | not separated | 0 | 0.905 | 23 | 0 | 27 | 0.0 | 83.7 | 0.0 | 1.93 | 27 | 5.0 | 15.6 | 0 |
| zipf_sorted | PCC | 3 | 7.513 | 0.023 | 1.364 | (1.252, 1.467) | separated, higher | 0 | 1.364 | 23 | 2 | 25 | 3.303 | 80.4 | 68.6 | 3.86 | 25 | 4.7 | 14.6 | 0 |
| zipf_sorted | PCC_INLINE | 3 | 4.974 | 0.026 | 0.903 | (0.83, 0.977) | not separated | 0 | 0.903 | 23 | 2 | 25 | 0.0 | 80.4 | 0.0 | 1.93 | 25 | 4.7 | 14.6 | 0 |
| zipf_skewdata | NO_CACHE | 3 | 6.521 | 0.042 | None | (None, None) | baseline | 15 | None | 0 | 0 | 0 | 0.0 | 1421.5 | 0.0 | 26.95 | 0 | 0.0 | 0.0 | 0 |
| zipf_skewdata | NAIVE_CACHE | 3 | 5.474 | 0.062 | 0.839 | (0.723, 0.95) | not separated | 15 | 1.049 | 11 | 0 | 39 | 0.0 | 1108.1 | 0.0 | 21.01 | 39 | 2.4 | 11.3 | 0 |
| zipf_skewdata | EXACT_FILE | 3 | 5.887 | 0.086 | 0.903 | (0.773, 1.018) | not separated | 15 | 1.146 | 11 | 0 | 39 | 0.0 | 1108.1 | 0.0 | 21.01 | 39 | 57.6 | 141.5 | 0 |
| zipf_skewdata | PCC | 3 | 8.779 | 0.095 | 1.346 | (1.138, 1.539) | separated, higher | 15 | 1.676 | 11 | 7 | 32 | 3.985 | 950.6 | 950.6 | 36.04 | 38 | 52.1 | 128.1 | 0 |
| zipf_skewdata | PCC_INLINE | 3 | 5.69 | 0.16 | 0.873 | (0.742, 0.984) | not separated | 15 | 1.098 | 11 | 7 | 32 | 0.0 | 950.6 | 0.0 | 18.02 | 38 | 52.1 | 128.1 | 0 |
| official_q6_random | NO_CACHE | 3 | 10.414 | 0.329 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 4186.2 | 0.0 | 300.06 | 0 | 0.0 | 0.0 | 0 |
| official_q6_random | NAIVE_CACHE | 3 | 8.129 | 0.128 | 0.781 | (0.66, 0.885) | separated, lower | 0 | 0.781 | 14 | 0 | 36 | 0.0 | 3014.1 | 0.0 | 216.04 | 36 | 2.6 | 11.3 | 0 |
| official_q6_random | EXACT_FILE | 3 | 13.279 | 0.227 | 1.275 | (1.2, 1.365) | separated, higher | 0 | 1.275 | 14 | 0 | 36 | 0.0 | 4186.2 | 0.0 | 300.06 | 36 | 384.8 | 922.5 | 0 |
| official_q6_random | PCC | 3 | 17.645 | 0.222 | 1.694 | (1.633, 1.75) | separated, higher | 0 | 1.694 | 0 | 0 | 50 | 8.15 | 4186.2 | 2547.8 | 600.12 | 0 | 0.0 | 0.0 | 0 |
| official_q6_random | PCC_INLINE | 3 | 11.346 | 0.075 | 1.089 | (1.052, 1.123) | separated, higher | 0 | 1.089 | 0 | 0 | 50 | 0.0 | 4186.2 | 0.0 | 300.06 | 0 | 0.0 | 0.0 | 0 |
| official_q1_sorted | NO_CACHE | 3 | 8.184 | 0.16 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 1443.5 | 0.0 | 118.26 | 0 | 0.0 | 0.0 | 0 |
| official_q1_sorted | NAIVE_CACHE | 3 | 7.389 | 0.344 | 0.903 | (0.778, 0.996) | not separated | 0 | 0.903 | 2 | 0 | 18 | 0.0 | 1299.6 | 0.0 | 106.48 | 18 | 3.7 | 10.4 | 0 |
| official_q1_sorted | EXACT_FILE | 3 | 9.93 | 0.178 | 1.213 | (1.16, 1.28) | separated, higher | 0 | 1.213 | 2 | 0 | 18 | 0.0 | 1443.4 | 0.0 | 118.26 | 18 | 190.2 | 456.2 | 0 |
| official_q1_sorted | PCC | 3 | 13.402 | 0.118 | 1.638 | (1.564, 1.7) | separated, higher | 0 | 1.638 | 0 | 0 | 20 | 5.66 | 1443.5 | 807.6 | 236.52 | 0 | 0.0 | 0.0 | 0 |
| official_q1_sorted | PCC_INLINE | 3 | 9.414 | 0.185 | 1.15 | (1.119, 1.179) | separated, higher | 0 | 1.15 | 0 | 0 | 20 | 0.0 | 1443.5 | 0.0 | 118.26 | 0 | 0.0 | 0.0 | 0 |
| zipf_q1_sorted | NO_CACHE | 3 | 4.212 | 0.119 | None | (None, None) | baseline | 0 | None | 0 | 0 | 0 | 0.0 | 74.8 | 0.0 | 1.43 | 0 | 0.0 | 0.0 | 0 |
| zipf_q1_sorted | NAIVE_CACHE | 3 | 3.554 | 0.113 | 0.844 | (0.716, 0.958) | not separated | 0 | 0.844 | 6 | 0 | 24 | 0.0 | 60.2 | 0.0 | 1.17 | 24 | 2.6 | 9.3 | 0 |
| zipf_q1_sorted | EXACT_FILE | 3 | 4.035 | 0.014 | 0.958 | (0.904, 1.003) | not separated | 0 | 0.958 | 6 | 0 | 24 | 0.0 | 65.1 | 0.0 | 1.43 | 24 | 4.8 | 14.6 | 0 |
| zipf_q1_sorted | PCC | 3 | 5.404 | 0.073 | 1.283 | (1.153, 1.415) | separated, higher | 0 | 1.283 | 4 | 11 | 15 | 2.173 | 50.6 | 42.4 | 2.87 | 15 | 2.7 | 8.4 | 0 |
| zipf_q1_sorted | PCC_INLINE | 3 | 3.608 | 0.011 | 0.857 | (0.781, 0.934) | separated, lower | 0 | 0.857 | 4 | 11 | 15 | 0.0 | 50.6 | 0.0 | 1.43 | 15 | 2.7 | 8.4 | 0 |
| threshold100_official_q6_random | PCC_INLINE | 1 | 13.823 | None | 1.327 | (1.234, 1.441) | separated, higher | 0 | 1.327 | 12 | 8 | 30 | 0.0 | 4186.2 | 0.0 | 300.06 | 30 | 320.6 | 768.8 | 0 |
| budget25_volume_sorted | PCC_INLINE | 1 | 5.807 | None | 1.044 | (0.995, 1.094) | not separated | 0 | 1.044 | 2 | 4 | 44 | 0.0 | 107.2 | 0.0 | 1.63 | 6 | 1.5 | 4.5 | 38 |
| budget25_coverage_sorted | PCC_INLINE | 1 | 5.693 | None | 1.023 | (0.942, 1.1) | not separated | 0 | 1.023 | 8 | 4 | 38 | 0.0 | 97.4 | 0.0 | 1.63 | 9 | 1.5 | 4.7 | 29 |
| budget50_volume_sorted | PCC_INLINE | 1 | 5.575 | None | 1.002 | (0.936, 1.065) | not separated | 0 | 1.002 | 6 | 6 | 38 | 0.0 | 97.5 | 0.0 | 1.63 | 14 | 2.9 | 9.2 | 24 |
| budget50_coverage_sorted | PCC_INLINE | 1 | 5.38 | None | 0.967 | (0.898, 1.035) | not separated | 0 | 0.967 | 11 | 5 | 34 | 0.0 | 90.9 | 0.0 | 1.63 | 19 | 3.0 | 9.6 | 15 |
| budget75_volume_sorted | PCC_INLINE | 1 | 5.348 | None | 0.961 | (0.894, 1.025) | not separated | 0 | 0.961 | 10 | 7 | 33 | 0.0 | 89.4 | 0.0 | 1.63 | 23 | 4.4 | 13.8 | 10 |
| budget75_coverage_sorted | PCC_INLINE | 1 | 5.288 | None | 0.95 | (0.881, 1.021) | not separated | 0 | 0.95 | 11 | 7 | 32 | 0.0 | 87.7 | 0.0 | 1.63 | 26 | 4.4 | 14.1 | 6 |
| budget25_volume_skewqty | PCC_INLINE | 1 | 6.653 | None | 1.02 | (0.924, 1.108) | not separated | 15 | 1.14 | 4 | 3 | 43 | 0.0 | 1247.7 | 0.0 | 23.66 | 5 | 13.0 | 31.8 | 40 |
| budget25_coverage_skewqty | PCC_INLINE | 1 | 5.794 | None | 0.888 | (0.761, 1.004) | not separated | 15 | 1.114 | 11 | 2 | 37 | 0.0 | 1061.4 | 0.0 | 20.12 | 21 | 12.8 | 32.9 | 17 |
| budget50_volume_skewqty | PCC_INLINE | 1 | 6.13 | None | 0.94 | (0.82, 1.053) | not separated | 15 | 1.111 | 8 | 6 | 36 | 0.0 | 1060.5 | 0.0 | 20.11 | 9 | 26.0 | 63.1 | 32 |
| budget50_coverage_skewqty | PCC_INLINE | 1 | 5.786 | None | 0.887 | (0.758, 1.008) | not separated | 15 | 1.112 | 11 | 5 | 34 | 0.0 | 989.9 | 0.0 | 18.77 | 29 | 25.7 | 64.5 | 9 |
| budget75_volume_skewqty | PCC_INLINE | 1 | 5.891 | None | 0.903 | (0.771, 1.016) | not separated | 15 | 1.118 | 10 | 6 | 34 | 0.0 | 1003.6 | 0.0 | 19.03 | 18 | 39.0 | 95.3 | 21 |
| budget75_coverage_skewqty | PCC_INLINE | 1 | 5.786 | None | 0.887 | (0.757, 1.018) | not separated | 15 | 1.107 | 11 | 6 | 33 | 0.0 | 968.8 | 0.0 | 18.37 | 34 | 39.0 | 96.4 | 4 |

| cell | first | second | ratio | 95% interval | reading |
|---|---|---|---|---|---|
| paper_generator_seed1 | PCC_INLINE | PCC | 0.665 | (0.648, 0.682) | separated, lower |
| paper_generator_seed1 | PCC_INLINE | EXACT_FILE | 0.776 | (0.734, 0.819) | separated, lower |
| paper_generator_seed1 | PCC | EXACT_FILE | 1.167 | (1.09, 1.241) | separated, higher |
| paper_generator_seed1 | NAIVE_CACHE | PCC_INLINE | 1.165 | (1.097, 1.24) | separated, higher |
| official_q6_sorted | PCC_INLINE | PCC | 0.641 | (0.611, 0.678) | separated, lower |
| official_q6_sorted | PCC_INLINE | EXACT_FILE | 0.974 | (0.944, 1.005) | not separated |
| official_q6_sorted | PCC | EXACT_FILE | 1.52 | (1.44, 1.595) | separated, higher |
| official_q6_sorted | NAIVE_CACHE | PCC_INLINE | 0.667 | (0.558, 0.764) | separated, lower |
| zipf_contained_sorted | PCC_INLINE | PCC | 0.659 | (0.643, 0.674) | separated, lower |
| zipf_contained_sorted | PCC_INLINE | EXACT_FILE | 0.94 | (0.891, 0.984) | not separated |
| zipf_contained_sorted | PCC | EXACT_FILE | 1.428 | (1.344, 1.501) | separated, higher |
| zipf_contained_sorted | NAIVE_CACHE | PCC_INLINE | 0.861 | (0.775, 0.947) | separated, lower |
| share_repeat0_sorted | PCC_INLINE | EXACT_FILE | 0.826 | (0.773, 0.883) | separated, lower |
| share_repeat30_sorted | PCC_INLINE | EXACT_FILE | 0.914 | (0.858, 0.968) | not separated |
| share_repeat60_sorted | PCC_INLINE | EXACT_FILE | 1.0 | (0.98, 1.02) | not separated |
| paper_generator_seed2 | PCC_INLINE | PCC | 0.67 | (0.652, 0.688) | separated, lower |
| paper_generator_seed2 | PCC_INLINE | EXACT_FILE | 0.822 | (0.775, 0.87) | separated, lower |
| paper_generator_seed2 | PCC | EXACT_FILE | 1.227 | (1.145, 1.316) | separated, higher |
| paper_generator_seed2 | NAIVE_CACHE | PCC_INLINE | 1.101 | (1.039, 1.174) | not separated |
| paper_generator_seed3 | PCC_INLINE | PCC | 0.674 | (0.661, 0.688) | separated, lower |
| paper_generator_seed3 | PCC_INLINE | EXACT_FILE | 0.819 | (0.765, 0.87) | separated, lower |
| paper_generator_seed3 | PCC | EXACT_FILE | 1.215 | (1.126, 1.304) | separated, higher |
| paper_generator_seed3 | NAIVE_CACHE | PCC_INLINE | 1.103 | (1.034, 1.183) | not separated |
| paper_rounded_random | PCC_INLINE | PCC | 0.673 | (0.653, 0.694) | separated, lower |
| paper_rounded_random | PCC_INLINE | EXACT_FILE | 0.817 | (0.774, 0.865) | separated, lower |
| paper_rounded_random | PCC | EXACT_FILE | 1.214 | (1.14, 1.299) | separated, higher |
| paper_rounded_random | NAIVE_CACHE | PCC_INLINE | 0.906 | (0.754, 1.039) | not separated |
| paper_test_ranges_random | PCC_INLINE | PCC | 0.604 | (0.574, 0.633) | separated, lower |
| paper_test_ranges_random | PCC_INLINE | EXACT_FILE | 0.185 | (0.075, 0.309) | separated, lower |
| paper_test_ranges_random | PCC | EXACT_FILE | 0.306 | (0.126, 0.512) | separated, lower |
| paper_test_ranges_random | NAIVE_CACHE | PCC_INLINE | 5.457 | (3.238, 13.47) | separated, higher |
| zipf_sorted | PCC_INLINE | PCC | 0.662 | (0.64, 0.685) | separated, lower |
| zipf_sorted | PCC_INLINE | EXACT_FILE | 0.998 | (0.959, 1.032) | not separated |
| zipf_sorted | PCC | EXACT_FILE | 1.507 | (1.438, 1.564) | separated, higher |
| zipf_sorted | NAIVE_CACHE | PCC_INLINE | 0.682 | (0.557, 0.781) | separated, lower |
| zipf_skewdata | PCC_INLINE | PCC | 0.648 | (0.632, 0.665) | separated, lower |
| zipf_skewdata | PCC_INLINE | EXACT_FILE | 0.967 | (0.923, 1.008) | not separated |
| zipf_skewdata | PCC | EXACT_FILE | 1.491 | (1.408, 1.567) | separated, higher |
| zipf_skewdata | NAIVE_CACHE | PCC_INLINE | 0.962 | (0.917, 1.017) | not separated |
| official_q6_random | PCC_INLINE | PCC | 0.643 | (0.627, 0.66) | separated, lower |
| official_q6_random | PCC_INLINE | EXACT_FILE | 0.854 | (0.795, 0.913) | separated, lower |
| official_q6_random | PCC | EXACT_FILE | 1.329 | (1.237, 1.42) | separated, higher |
| official_q6_random | NAIVE_CACHE | PCC_INLINE | 0.716 | (0.611, 0.806) | separated, lower |
| official_q1_sorted | PCC_INLINE | PCC | 0.702 | (0.682, 0.724) | separated, lower |
| official_q1_sorted | PCC_INLINE | EXACT_FILE | 0.948 | (0.899, 0.995) | not separated |
| official_q1_sorted | PCC | EXACT_FILE | 1.35 | (1.267, 1.43) | separated, higher |
| official_q1_sorted | NAIVE_CACHE | PCC_INLINE | 0.785 | (0.677, 0.861) | separated, lower |
| zipf_q1_sorted | PCC_INLINE | PCC | 0.668 | (0.648, 0.692) | separated, lower |
| zipf_q1_sorted | PCC_INLINE | EXACT_FILE | 0.894 | (0.823, 0.966) | not separated |
| zipf_q1_sorted | PCC | EXACT_FILE | 1.339 | (1.221, 1.453) | separated, higher |
| zipf_q1_sorted | NAIVE_CACHE | PCC_INLINE | 0.985 | (0.843, 1.121) | not separated |
| budget25_coverage_sorted | coverage | volume | 0.98 | (0.903, 1.057) | not separated |
| budget50_coverage_sorted | coverage | volume | 0.965 | (0.903, 1.025) | not separated |
| budget75_coverage_sorted | coverage | volume | 0.989 | (0.957, 1.02) | not separated |
| budget25_coverage_skewqty | coverage | volume | 0.871 | (0.78, 0.952) | not separated |
| budget50_coverage_skewqty | coverage | volume | 0.944 | (0.867, 1.009) | not separated |
| budget75_coverage_skewqty | coverage | volume | 0.982 | (0.93, 1.035) | not separated |

Problems: {'mismatches': 0, 'failed_runs': 0, 'missing_runs': 0, 'short_runs': 0, 'hit_disagreement': 0, 'identity_mismatch': 0, 'skipped_runs': 0}
Problem details: {}
A/A control: [{'cell': 'paper_generator_seed1', 'ratio': 1.001, 'ci95': (0.981, 1.02), 'timing': 'not separated'}, {'cell': 'zipf_contained_sorted', 'ratio': 0.994, 'ci95': (0.975, 1.013), 'timing': 'not separated'}]
Identity: {'git_sha': '8977106f315aa34faa6ea01f1b27dfb3bdaa6a01', 'jar_sha256': '3afac74cc61c3a255fdb9bee2710f855e9eab29667210864c3f7238a28127e92', 'plan_sha256': 'ce7ae15d05a187f02583a4bc0ff96ae42e13801b0598b6e523ecbe096d2c2270'}
