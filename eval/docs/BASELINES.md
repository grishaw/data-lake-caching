# Baselines and how they differ from Schmidt et al. (SIGMOD 2024)

Label results "Schmidt-inspired", never "Schmidt". The Redshift prototype is not available to us and we found no public code.

| Method | Stores | Reuse rule | On a hit | From Schmidt | Differs from Schmidt |
|---|---|---|---|---|---|
| `NO_CACHE` | Nothing | None | Spark with normal pruning | Nothing | Baseline only |
| `NAIVE_CACHE` | Query result | Exact predicate | No read | Nothing | A result cache, which Schmidt's method is not |
| `EXACT_FILE` | Qualifying files | Exact predicate | Scans those files, re-evaluates the predicate | Exact key; stores where rows are; re-evaluates; admits every predicate; built during the scan | File granularity, while Schmidt keeps row ranges or 1,000-row block bitmaps. Built through a Spark group-by, not inside the scan operator. Predicate text is compared as values. No join or semi-join capture, no handling of inserts. Not in Redshift |
| `PCC` | Qualifying files | Interval containment | Scans tightest containing entry, then scans again for coverage | Not applicable | The paper's method, with the original's two scans |
| `PCC_INLINE` | Qualifying files | Interval containment | Scans tightest containing entry; coverage in the same scan | Not applicable | Same cache as `PCC`, one scan. A changed method |

`EXACT_FILE` is a file-level control for containment. Schmidt's section 4.5 says a predicate cache can index qualifying Parquet files, which is the defence for this design.
It is not a state-of-the-art baseline, and it cannot show how PCC compares with a block-level cache. File granularity removes Schmidt's main strength, and every main layout has one
row group per file, so a row-group version would collapse to file level on these layouts. Its one-scan collection costs more than Schmidt's reported 0.5% build overhead,
so its cost here is an upper bound on what a real scan-level implementation would pay.
On layouts where predicates touch most files, every file-level method is weak by construction, so those cells say little about containment.
Continuous bounds in the paper's generator give every exact method 0 hits. `paper_rounded_random` clips bounds to the data so equal predicates compare equal, which is the fair version of that comparison.
Whether this control is enough as a state-of-the-art baseline is a question for Grisha and Ehud.
