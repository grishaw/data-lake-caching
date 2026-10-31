# What was done

Local evaluation of "Predicate Containment Caching in Cloud Data Lakes" before one cloud run. Nothing here ran on AWS.

## What changed

`src/` is identical to commit `581036c`. Everything new is under `eval/`. See `CHANGES.md` for each difference from the original.

## What actually ran

| Check | Result | Evidence |
|---|---|---|
| Unchanged original | Finishes at SF10. Crashes on DECIMAL columns, and on empty results at SF1 | `results/stage_a_original/` |
| Comparison, one commit | 226 of 226 runs, 0 wrong answers against an independent oracle, 0 coverage errors | `results/local/summary.md` |
| A/A control | 1.00 and 0.99, not separated | `results/local/summary.md` |
| Held-out trace, run once | 15 of 15 runs, 0 wrong answers. Same pattern as tuned traces | `results/local_heldout/summary.md` |
| Extra batch, run twice | 53 of 53 runs each time, 0 wrong answers. Skewed mixes with all methods and a 10% budget. Cite `local_extra_quiet`; the first pass ran on a busy machine. Drift remained: no-cache SD 7 to 24%, and one-repeat 10% budget cells flipped between passes | `results/local_extra_quiet/summary.md` |
| A/A on the skewed layout | 0.98, not separated | `results/local_aa_skew/summary.md` |

Not tested: real EMR, EMRFS, YARN, IAM on the real account, and real S3 latency or request counts.

## Regressions and surprises

1. The original's second scan is outside its timer. With it counted, two-scan PCC costs 1.22 to 1.82 times plain Spark in 11 of 12 local workloads.
2. A one-scan variant costs 0.82 to 0.95 in 11 of 15 workloads and 1.09 to 1.17 in the 3 wide-predicate ones. It is a changed method. On skewed data 15 of 50 queries are empty and every exact hit repeats one, so on queries with rows it costs 1.10 (`ratio_rows_only`).
3. On skewed mixes, where files read can fall, one scan planned 66 files against 91 for exact files. Its time was not separated from no cache on any mix. It was lower than exact files on one mix in one of two passes, and most of that gap came from the threshold barring repeats, not from containment.
4. The repo's test-default ranges return no rows on all 50 queries at SF1, so their 0.18 says nothing about speed. Test and README ranges both fix every lower bound, so queries nest by design.
5. Sorted files leave nothing to save at file level, because min/max pruning already gets to about 1.4 files per query.
6. S3 request charges may match or exceed compute. The cloud run measures them.

## Next step

The cloud runs come in a separate PR.
