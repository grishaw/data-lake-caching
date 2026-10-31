#!/usr/bin/env python3
"""Checks every answer and summarises a results directory. Exits 1 if anything is wrong or missing.

Usage: analyze.py <results dir> <plan.json>
Answers are checked against the independent oracle. Needs local data and pyarrow.

Timing rule, stated before the run:
A method's cost is reported as a ratio of total workload to NO_CACHE in the same cell, with a 95% bootstrap interval over queries,
using per-query means across repeats, from one commit only. The same is reported for the direct pairs in PAIRS.
"separated" means the whole interval lies outside 1 +/- MIN_EFFECT. Otherwise "not separated".
A/A cells (NO_CACHE against a second NO_CACHE) show how often that rule fires on no real difference. The interval covers query-to-query
variation, not run-to-run drift, so it can be too narrow with one repeat. These words describe one run of one setup. They support no general claim.
MIN_EFFECT must be reset from the pilot's A/A runs before the full run is read.
"""
import json
import pathlib
import random
import statistics as st
import subprocess
import sys
import zlib
from decimal import Decimal

MIN_EFFECT = 0.05
SUM_REL_TOL = Decimal("1e-12")
SUM_ABS_TOL = Decimal("1e-6")
results = pathlib.Path(sys.argv[1])
plan = json.load(open(sys.argv[2]))
problems = dict(mismatches=[], failed_runs=[], missing_runs=[], short_runs=[], hit_disagreement=[], identity_mismatch=[], skipped_runs=[])
PAIRS = [("PCC_INLINE", "PCC"), ("PCC_INLINE", "EXACT_FILE"), ("PCC", "EXACT_FILE"), ("NAIVE_CACHE", "PCC_INLINE")]
BOOTSTRAPS = 2000


def load_jsonl(path):
    return [json.loads(x) for x in open(path) if x.strip()]


def oracle_for(cell):
    path = results / "_oracle" / f"{pathlib.Path(cell['layout']).name}__{pathlib.Path(cell['trace']).stem}.jsonl"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["python3", "scripts/oracle.py", cell["layout"], cell["trace"], str(path)], check=True)
    return {r["seq"]: r for r in load_jsonl(path)}


def same_sum(a, b):
    if a is None or b is None:
        return a is None and b is None
    a, b = Decimal(a), Decimal(b)
    return abs(a - b) <= max(SUM_ABS_TOL, abs(b) * SUM_REL_TOL)


def per_query(runs, rows_only=False):
    keep = [i for i, q in enumerate(runs[0]["queries"]) if q["result_count"] > 0 or not rows_only]
    return [st.mean(r["queries"][i]["workload_ns"] for r in runs) for i in keep]


def ratio_interval(a_runs, b_runs, seed, rows_only=False):
    """Ratio of total workload with a bootstrap interval over queries, paired because both replay the same queries."""
    a, b = per_query(a_runs, rows_only), per_query(b_runs, rows_only)
    n = len(a)
    rng = random.Random(seed)
    samples = []
    for _ in range(BOOTSTRAPS):
        picks = [rng.randrange(n) for _ in range(n)]
        samples.append(sum(a[i] for i in picks) / sum(b[i] for i in picks))
    samples.sort()
    return round(sum(a) / sum(b), 3), round(samples[int(0.025 * BOOTSTRAPS)], 3), round(samples[int(0.975 * BOOTSTRAPS)], 3)


def describe(low, high):
    if high < 1 - MIN_EFFECT:
        return "separated, lower"
    if low > 1 + MIN_EFFECT:
        return "separated, higher"
    return "not separated"


summary = dict(cells=[], pairs=[], table3=[], identity=json.load(open(results / "identity.json")) if (results / "identity.json").exists() else None)
loaded = {}
for cell in plan["cells"]:
    trace_len = sum(1 for x in open(cell["trace"]) if x.strip())
    methods = cell.get("methods", plan["methods"])
    repeats = cell.get("repeats", plan["repeats"])
    oracle = oracle_for(cell)
    runs = {}
    for label in methods:
        for rep in range(repeats):
            d = results / cell["name"] / label / f"rep{rep}"
            where = dict(cell=cell["name"], method=label, rep=rep)
            if not (d / "run.json").exists():
                problems["missing_runs"].append(where)
                continue
            info = json.load(open(d / "run.json"))
            if summary["identity"]:
                args = info.get("args", {})
                if (args.get("git-sha"), args.get("jar-sha256"), args.get("plan-sha256")) != (
                        summary["identity"]["git_sha"], summary["identity"]["jar_sha256"], summary["identity"]["plan_sha256"]) and info.get("status") == "completed":
                    problems["identity_mismatch"].append(where)
            if info.get("status") != "completed":
                problems["failed_runs"].append(dict(where, status=info.get("status"), error=info.get("error")))
                continue
            qs = load_jsonl(d / "queries.jsonl")
            if len(qs) != trace_len:
                problems["short_runs"].append(dict(where, rows=len(qs), expected=trace_len))
                continue
            runs.setdefault(label, []).append(dict(info=info, queries=qs))
    loaded[cell["name"]] = runs
    # every run of every method answers every query correctly, or it is reported
    for label, reps in runs.items():
        for k, run in enumerate(reps):
            for i, q in enumerate(run["queries"]):
                want = oracle[q["seq"]]
                bad = q["result_count"] != want["count"] or not same_sum(q["result_sum"], want["sum"])
                groups = want["groups"]
                if q.get("result_groups") or groups:
                    for g, value in q["result_groups"].items():
                        bad = bad or g not in groups or int(value[0]) != groups[g][0] or not same_sum(str(value[1]), groups[g][1])
                    bad = bad or set(q["result_groups"]) != set(groups)
                if q["coverage_size"] >= 0:
                    bad = bad or q["coverage_size"] != len(want["coverage"]) or q["coverage_sha256"] != want["coverage_sha256"]
                elif label.split("@")[0] in ("PCC", "PCC_INLINE", "EXACT_FILE"):
                    # no coverage is fine only for an exact hit, or a hit on an empty entry that read nothing
                    bad = bad or not (q["hit_type"] == "EXACT" or (q["hit_type"] == "CONTAINED" and q["files_planned"] == 0))
                if bad:
                    problems["mismatches"].append(dict(cell=cell["name"], method=label, rep=k, seq=q["seq"], got=[q["result_count"], q["result_sum"], q["coverage_size"]]))
        # cache behaviour is deterministic, so repeats must agree exactly
        hits = {tuple(q["hit_type"] for q in r["queries"]) for r in reps}
        if len(hits) > 1:
            problems["hit_disagreement"].append(dict(cell=cell["name"], method=label))
    # the two PCC methods share cache rules, so with the same budget they must hit the same queries
    if "PCC" in runs and "PCC_INLINE" in runs and [q["hit_type"] for q in runs["PCC"][0]["queries"]] != [q["hit_type"] for q in runs["PCC_INLINE"][0]["queries"]]:
        problems["hit_disagreement"].append(dict(cell=cell["name"], method="PCC against PCC_INLINE"))

    base = runs.get("NO_CACHE") or []
    base_cell = cell["name"]
    if not base and cell.get("baseline_of"):
        base = loaded.get(cell["baseline_of"], {}).get("NO_CACHE", [])
        base_cell = cell["baseline_of"]
    base_w = [sum(q["workload_ns"] for q in r["queries"]) / 1e9 for r in base]
    for label, reps in runs.items():
        w = [sum(q["workload_ns"] for q in r["queries"]) / 1e9 for r in reps]
        no_first = [sum(q["workload_ns"] for q in r["queries"][1:]) / 1e9 for r in reps]
        lat = [sum(q["latency_ns"] for q in r["queries"]) / 1e9 for r in reps]
        cov = [sum(q["coverage_ns"] for q in r["queries"]) / 1e9 for r in reps]
        qs = reps[0]["queries"]
        n = len(qs)
        seed = zlib.crc32(f"{cell['name']}|{label}".encode()) % 1000  # hash() changes per process, so reruns would differ
        ratio = ratio_interval(reps, base, seed) if base and label != "NO_CACHE" else (None, None, None)
        # a hit on an empty query reads nothing, so it can flatter a whole workload
        with_rows = ratio_interval(reps, base, seed, True) if base and label != "NO_CACHE" and any(q["result_count"] for q in qs) else (None, None, None)
        kinds = {}
        for q in qs:
            kinds[q["hit_type"]] = kinds.get(q["hit_type"], 0) + 1
        summary["cells"].append(dict(
            cell=cell["name"], answers=cell.get("answers"), method=label, repeats=len(reps), queries=n,
            workload_s_mean=round(st.mean(w), 3), workload_s_sd=round(st.stdev(w), 3) if len(w) > 1 else None,
            workload_s_min=round(min(w), 3), workload_s_max=round(max(w), 3),
            workload_s_mean_without_first_query=round(st.mean(no_first), 3),
            ratio_to_no_cache=ratio[0], ratio_ci95=ratio[1:], baseline_cell=base_cell if base else None,
            empty_queries=sum(q["result_count"] == 0 for q in qs), ratio_rows_only=with_rows[0], ratio_rows_only_ci95=with_rows[1:],
            timing="baseline" if label == "NO_CACHE" else describe(ratio[1], ratio[2]) if base else None,
            latency_s_mean=round(st.mean(lat), 3), coverage_s_mean=round(st.mean(cov), 3),
            exact_hits=kinds.get("EXACT", 0), contained_hits=kinds.get("CONTAINED", 0), misses=kinds.get("MISS", 0),
            hit_ratio=round((kinds.get("EXACT", 0) + kinds.get("CONTAINED", 0)) / n, 3) if n else None,
            scan_mb=round(sum(q["scan_bytes_read"] for q in qs) / 1e6, 1),
            coverage_mb=round(sum(q["coverage_bytes_read"] for q in qs) / 1e6, 1),
            records_read_m=round(sum(q["scan_records_read"] + q["coverage_records_read"] for q in qs) / 1e6, 2),
            mean_files_planned=round(st.mean(q["files_planned"] for q in qs), 1),
            cache_entries=qs[-1]["cache_entries"], cache_kb=round(qs[-1]["cache_bytes"] / 1024, 1),
            cache_peak_kb=round(max(q["cache_bytes"] for q in qs) / 1024, 1),
            cache_heap_kb=round(qs[-1]["cache_heap_bytes"] / 1024, 1), cache_heap_peak_kb=round(max(q["cache_heap_bytes"] for q in qs) / 1024, 1),
            evictions=reps[0]["info"].get("evictions"),
            capacity_bytes=reps[0]["info"].get("capacity_bytes"),
        ))

    for a, b in PAIRS:
        if a in runs and b in runs:
            r = ratio_interval(runs[a], runs[b], 7)
            summary["pairs"].append(dict(cell=cell["name"], first=a, second=b, ratio=r[0], ci95=r[1:], reading=describe(r[1], r[2])))

# coverage and volume budgets live in separate cells, so their direct comparison is paired here
for cell in plan["cells"]:
    twin = cell["name"].replace("_coverage_", "_volume_")
    if twin != cell["name"] and "PCC_INLINE" in loaded[cell["name"]] and "PCC_INLINE" in loaded.get(twin, {}):
        r = ratio_interval(loaded[cell["name"]]["PCC_INLINE"], loaded[twin]["PCC_INLINE"], 7)
        summary["pairs"].append(dict(cell=cell["name"], first="coverage", second="volume", ratio=r[0], ci95=r[1:], reading=describe(r[1], r[2])))

# one 100-query run gives Table 3's 10, 50 and 100 query rows; paper_timer leaves out the coverage scan, as Table 3 does
for cell in plan["cells"]:
    if cell["name"].startswith("table3_"):
        for label, reps in loaded[cell["name"]].items():
            for n in (10, 50, 100):
                total = lambda field: round(st.mean(sum(q[field] for q in r["queries"][:n]) for r in reps) / 1e9, 1)
                summary["table3"].append(dict(cell=cell["name"], method=label, queries=n, paper_timer_s=total("latency_ns"), workload_s=total("workload_ns")))

schedule = results / "schedule.jsonl"
if schedule.exists():
    for rec in load_jsonl(schedule):
        if rec.get("skipped"):
            problems["skipped_runs"].append(rec)

aa = [c for c in summary["cells"] if c["method"].endswith("@2")]
summary["aa_control"] = [dict(cell=c["cell"], ratio=c["ratio_to_no_cache"], ci95=c["ratio_ci95"], timing=c["timing"]) for c in aa]
summary["problems"] = {k: len(v) for k, v in problems.items()}
summary["problem_details"] = {k: v[:20] for k, v in problems.items() if v}
json.dump(summary, open(results / "summary.json", "w"), indent=1)

columns = ["cell", "method", "repeats", "workload_s_mean", "workload_s_sd", "ratio_to_no_cache", "ratio_ci95", "timing", "empty_queries", "ratio_rows_only", "exact_hits", "contained_hits",
           "misses", "coverage_s_mean", "scan_mb", "coverage_mb", "records_read_m", "cache_entries", "cache_peak_kb", "cache_heap_peak_kb", "evictions"]
with open(results / "summary.md", "w") as f:
    f.write("| " + " | ".join(columns) + " |\n|" + "---|" * len(columns) + "\n")
    for c in summary["cells"]:
        f.write("| " + " | ".join(str(c[k]) for k in columns) + " |\n")
    f.write("\n| cell | first | second | ratio | 95% interval | reading |\n|---|---|---|---|---|---|\n")
    for p in summary["pairs"]:
        f.write(f"| {p['cell']} | {p['first']} | {p['second']} | {p['ratio']} | {p['ci95']} | {p['reading']} |\n")
    for r in summary["table3"]:
        f.write(f"\nTable 3 {r['cell']} {r['method']} {r['queries']} queries: paper timer {r['paper_timer_s']} s, all costs {r['workload_s']} s")
    f.write(f"\nProblems: {summary['problems']}\nProblem details: {summary['problem_details']}\nA/A control: {summary['aa_control']}\nIdentity: {summary['identity']}\n")
print(open(results / "summary.md").read()[-600:])
sys.exit(1 if any(problems.values()) else 0)
