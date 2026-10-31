# Changes from the original benchmark

`src/`, `pom.xml` and `README.md` are untouched and identical to commit `581036c`. All new code is under `eval/`.
Whether `cloud.Benchmark` at that commit is exactly what produced Table 3 is for Grisha to confirm.

The evaluation has three separately labelled stages:

| Stage | Code | Question |
|---|---|---|
| A. Reproduction | Unchanged `cloud.Benchmark`, README arguments | Do we get near Table 3? |
| B. Corrected measurement | `ControlledRunner`, method `PCC` | Does the claim hold with every cost counted? |
| C. New experiments | `ControlledRunner`, other methods, workloads | Where does PCC help or lose? |

The controlled runner calls the original `ConditionValues`, `contains`, `equals` and `getFilesFromCache` directly: the eval jar includes the
original classes unmodified.

## Stage B differences from the original

| # | Original behaviour | Controlled runner | Why |
|---|---|---|---|
| 1 | Random queries per mode, not saved | Same saved trace for every method | Methods can be compared and reruns repeat |
| 2 | Time truncated to whole seconds | Nanoseconds per phase | Most SF10 queries print 0 |
| 3 | Coverage collection and insertion run after the timer stops | Timed and reported | The real cost has to be reported |
| 4 | Coverage job also runs on no-cache and naive runs | Only where the method uses coverage | No method pays for work it does not use |
| 5 | Empty result crashes on a null sum | Handled, and the empty-coverage branch can now run | Empty results are a real query class |
| 6 | Modes in fixed order in one JVM | Fresh JVM per run, independent random order per repeat | Reduces warm-cache and JIT bias. It cannot remove them: the OS page cache persists locally |
| 7 | Warm-up is one `show()` | Same warm-up for every run: plain, per-file and coverage plans on all files, timed apart | Controls start-up cost. Reads of a cached file list are not warmed |
| 8 | Result is only a sum | Count, sum, Q1 group values, coverage hash | Allows answer and coverage checks |
| 9 | Prints logs | One JSON row per query, written as it finishes | Reports hit ratio and size; a timeout keeps finished rows |
| 10 | Naive cache plans the query before lookup, inserts on hits, and runs a coverage job | Lookup first; a hit does no Spark work; miss only | What a result cache does |
| 11 | Unbounded cache in the cloud code | Optional byte budget, eviction by Eq. 5 | Allows several cache sizes |
| 12 | `collectAsList` on a typed sum | `collectAsList` on `Row` with count and sum | Needed for count; same scan |
| 13 | Threshold is an integer argument | Fraction of files, default 0.5, the README ratio | Same value at each size |

Eviction details the paper does not fix: a new entry takes part in scoring and may evict itself, min and max are recomputed over the cache at
each eviction instead of being kept in a heap, a byte budget can evict several entries per insert, and ties go to the oldest entry.

An entry's size is 36 predicate bytes plus 4 bytes and the path length per file. When volume or coverage has no spread across the cache, that term scores 0.
Timed phases exclude harness work between queries: metric draining, plan capture and the cache heap estimate.

Measurement choices: `files_planned` is the files given to Spark, not files opened. On a miss it is every file, and Spark's Parquet statistics
may skip reading most of them, so read `scan_mb` and `records_read_m` for I/O. Local runs use 12 shuffle partitions and `local[6]`.

## Stage C additions

- `EXACT_FILE` stores qualifying files for an exact predicate, with no admission threshold, and learns them in the same scan on a miss. An exact hit is a plain scan of the stored files.
- `PCC_INLINE` is the same cache, lookup and threshold as `PCC`, but one `groupBy(input_file_name())` scan returns the answer and the coverage together.
  An exact hit is a plain scan, since coverage is already tight. It is a changed method and it is reported separately. The paper never evaluated it, so it must be disclosed as such.
  Its collection cost sits inside `exec_ns`, with `coverage_ns` zero. Budget cells run only this method; the two PCC methods hit the same queries, which the analyzer enforces.
- Query templates beyond Q6: Q1 with its real group-by, with official parameters (nearly every file qualifies) and with selective parameters.
- Workloads: the paper's generator with three seeds, the ranges in the repo's test defaults, the same generator with bounds clipped to the data so exact matching gets a fair chance,
  official Q6, Zipf with and without repeats, and traces with a stated share of repeats against containment. All are authored; none is a real query log.
  Every paper-generator query starts at 1998-12-01, the last ship date, so it can match at most that day's rows: 18 at SF1, about 219 at SF10.
- Layouts: `sorted` and `skewqty`, both ours. `skewqty` thins the table to 540,198 rows at SF1 (from 6,001,215), so it is smaller as well as skewed.

## Data

The unchanged `cloud.Benchmark` crashes on the spec's DECIMAL columns because `Encoders.DOUBLE` cannot up-cast a decimal sum. So the paper's Parquet files must have
had DOUBLE price and discount columns, and all our layouts use DOUBLE. The original layout script is missing, so the date column type (we use DATE) and the file
layouts are ours. dbgen comes from a community mirror; the SF1 Q6 answer matches the published 123141078.23.

Traces: 50 queries in controlled runs (20 and 30 for Q1), shorter than the paper's 100, to control cost. Stage A keeps 10, 50 and 100.
Early cache budgets (8,000 and 32,000 bytes) never forced an eviction. Budgets are now fractions of the unlimited cache size, set from the data at run time.

`results/` holds summaries, not the per-query rows. The runs were recorded before these commits were reorganized and the plan labels reworded, so the commit and plan hashes in them do not match this branch. The cells are unchanged.

## Not implemented

Spatial indexes in the controlled runner, joins, string containment, disk-based cache, and data, index or view comparisons.
The controlled runner has no timestamp rule for appended files, so its data must not change during a run.
