#!/usr/bin/env python3
"""Writes every run plan. Each cell says what it checks.

Listed by priority: repeat 0 of every cell runs first.
"""
import json
import os

METHODS = ["NO_CACHE", "NAIVE_CACHE", "EXACT_FILE", "PCC", "PCC_INLINE"]
SHARE = ["NO_CACHE", "EXACT_FILE", "PCC_INLINE"]
POLICIES = [("volume", 1, 0), ("coverage", 0, 1)]
AA = METHODS + ["NO_CACHE@2"]


def cells(data, files, traces, heldout):
    layout = lambda kind: f"{data}/{kind}_{files}"
    trace = lambda name: f"{traces}/{name}.jsonl"
    out = [
        # the paper's own query generator; seeds are the replication here, because one seed can be a lucky outlier
        dict(name="paper_generator_seed1", layout=layout("random"), trace=trace("smoke_paper_dec1998"), methods=AA, answers="ranges, hit ratios"),
        dict(name="official_q6_sorted", layout=layout("sorted"), trace=trace("smoke_official_q6"), answers="realistic parameters, Spark pruning on"),
        dict(name="zipf_contained_sorted", layout=layout("sorted"), trace=trace("smoke_zipf_contained"), methods=AA, answers="Zipfian popularity, no repeats, so only containment can hit"),
        dict(name="share_repeat0_sorted", layout=layout("sorted"), trace=trace("smoke_share_repeat0"), methods=SHARE, answers="gain beyond repeats: no exact repeats"),
        dict(name="share_repeat30_sorted", layout=layout("sorted"), trace=trace("smoke_share_repeat30"), methods=SHARE, repeats=1, answers="gain beyond repeats: half and half"),
        dict(name="share_repeat60_sorted", layout=layout("sorted"), trace=trace("smoke_share_repeat60"), methods=SHARE, repeats=1, answers="gain beyond repeats: repeats only"),
        dict(name="paper_generator_seed2", layout=layout("random"), trace=trace("smoke_paper_dec1998_s2"), repeats=1, answers="several seeds"),
        dict(name="paper_generator_seed3", layout=layout("random"), trace=trace("smoke_paper_dec1998_s3"), repeats=1, answers="several seeds"),
        dict(name="paper_rounded_random", layout=layout("random"), trace=trace("smoke_paper_rounded"), repeats=1, answers="fair exact matching: bounds clipped to the data"),
        dict(name="paper_test_ranges_random", layout=layout("random"), trace=trace("smoke_paper_test_ranges"), repeats=1, answers="the ranges in the test defaults"),
        dict(name="zipf_sorted", layout=layout("sorted"), trace=trace("smoke_zipf"), repeats=1, answers="Zipfian queries with repeats"),
        dict(name="zipf_skewdata", layout=layout("skewqty"), trace=trace("smoke_zipf_contained"), repeats=1, answers="skewed data"),
        dict(name="official_q6_random", layout=layout("random"), trace=trace("smoke_official_q6"), repeats=1, answers="weak case: wide predicates"),
        dict(name="official_q1_sorted", layout=layout("sorted"), trace=trace("smoke_official_q1"), repeats=1, answers="second template, weak case"),
        dict(name="zipf_q1_sorted", layout=layout("sorted"), trace=trace("smoke_zipf_q1"), repeats=1, answers="second template, selective"),
        dict(name="threshold100_official_q6_random", layout=layout("random"), trace=trace("smoke_official_q6"), repeats=1, methods=["PCC_INLINE"],
             baseline_of="official_q6_random", extra={"coverage-threshold": 1.0}, answers="threshold sensitivity"),
    ]
    for source, kind in (("zipf_contained_sorted", "sorted"), ("zipf_skewdata", "skewqty")):
        for fraction in (0.25, 0.5, 0.75):
            for label, w1, w2 in POLICIES:
                out.append(dict(name=f"budget{int(fraction * 100)}_{label}_{kind}", layout=layout(kind), trace=trace("smoke_zipf_contained"), repeats=1,
                                answers="several cache sizes", methods=["PCC_INLINE"], fraction_of=source, baseline_of=source,
                                extra={"capacity-fraction": fraction, "w1": w1, "w2": w2}))
    if heldout:
        # run once at the very end, never used to tune anything
        out.append(dict(name="heldout_mix_sorted", layout=layout("sorted"), trace=trace("heldout_mix"), answers="held-out check"))
    return out


def extra_cells(data, files, traces):
    # mixes on a layout where files read can fall, all methods; and the 10% budget, where policies differ most
    out = [dict(name=f"share_repeat{r}_skewdata", layout=f"{data}/skewqty_{files}", trace=f"{traces}/smoke_share_repeat{r}.jsonl",
                answers="gain beyond repeats where files read can fall") for r in (0, 30, 60)]
    for source, kind in (("zipf_contained_sorted", "sorted"), ("zipf_skewdata", "skewqty")):
        for label, w1, w2 in POLICIES:
            out.append(dict(name=f"budget10_{label}_{kind}", layout=f"{data}/{kind}_{files}", trace=f"{traces}/smoke_zipf_contained.jsonl", repeats=1,
                            answers="coverage against volume at a small budget", methods=["PCC_INLINE"], fraction_of=source, baseline_of=source,
                            extra={"capacity-fraction": 0.1, "w1": w1, "w2": w2}))
    return out


def table3_cells(data, traces, file_counts):
    # one 100-query run per file count: its first 10 and 50 queries give Table 3's other rows, so 3 runs cover 9 cells
    return [dict(name=f"table3_{n}", layout=f"{data}/random_{n}", trace=f"{traces}/paper_table3.jsonl", methods=["NO_CACHE", "NAIVE_CACHE", "PCC"],
                 answers="Grisha's Table 3 grid") for n in file_counts]


LOCAL_DATA = "/work/eval/data/sf1"
LOCAL_TRACES = "/work/eval/traces"
base = dict(seed=20261030, repeats=3, methods=METHODS)


def local_repeats(items):
    # locally every comparison gets three repeats, since a local run costs nothing
    return [{k: v for k, v in c.items() if k != "repeats" or "budget" in c["name"] or "threshold" in c["name"]} for c in items]


plans = {
    "local": dict(base, timeout_s=1800, cells=local_repeats(cells(LOCAL_DATA, 100, LOCAL_TRACES, False))),
    "local_heldout": dict(base, timeout_s=1800, cells=[c for c in cells(LOCAL_DATA, 100, LOCAL_TRACES, True) if c["name"].startswith("heldout")]),
    "local_table3": dict(base, timeout_s=1800, cells=table3_cells(LOCAL_DATA, LOCAL_TRACES, (100,))),
    "local_aa_skew": dict(base, timeout_s=1800, cells=[dict(name="aa_skewdata", layout=f"{LOCAL_DATA}/skewqty_100", trace=f"{LOCAL_TRACES}/smoke_share_repeat0.jsonl",
                                                            methods=["NO_CACHE", "NO_CACHE@2"], answers="noise floor of the skewed mixes")]),
    # a separate batch, so the existing local results keep their commit; the two source cells set the budgets
    "local_extra": dict(base, timeout_s=1800, cells=[dict(c, methods=["NO_CACHE", "PCC_INLINE"], repeats=1) for c in cells(LOCAL_DATA, 100, LOCAL_TRACES, False)
                                                     if c["name"] in ("zipf_contained_sorted", "zipf_skewdata")] + extra_cells(LOCAL_DATA, 100, LOCAL_TRACES)),
}
os.makedirs("plans", exist_ok=True)
for name, plan in plans.items():
    with open(f"plans/{name}.json", "w") as f:
        json.dump(plan, f, indent=1)
print({k: (len(v["cells"]), sum(c.get("repeats", v["repeats"]) * len(c.get("methods", v["methods"])) for c in v["cells"])) for k, v in plans.items()})
