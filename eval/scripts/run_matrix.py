#!/usr/bin/env python3
"""Runs a plan one configuration at a time, each in a fresh JVM, with a timeout. A failed run is recorded and never retried.

Usage: run_matrix.py <plan.json> <results dir> [repeat numbers, e.g. 0 or 1,2]
A run that cannot start is skipped and recorded, so one failure cannot end the job.
Repeat 0 of every cell finishes before repeat 1 of any cell, so a time limit loses repeats and not whole cells.
All runs in one results directory must come from one commit, jar and plan, or the script refuses to start.
Method order is an independent seeded random permutation per repeat. Saved in schedule.jsonl with real sequence and time.
Plan keys: seed, repeats, methods, timeout_s, cells; optional master, driver_memory, spark_conf, jar.
Cell keys: name, layout, trace, answers; optional repeats, methods, extra, fraction_of, baseline_of.
A method named NAME@2 runs NAME as a second independent copy, which is the A/A control.
fraction_of with extra capacity-fraction sets the byte budget from that cell's unlimited PCC_INLINE cache.
baseline_of names the cell whose NO_CACHE runs a single-method cell is compared with.
"""
import fcntl
import hashlib
import json
import os
import pathlib
import random
import subprocess
import sys
import time


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


plan_path = sys.argv[1]
plan = json.load(open(plan_path))
results = pathlib.Path(sys.argv[2])
results.mkdir(parents=True, exist_ok=True)
lock = open("/tmp/pcc-matrix.lock", "w")
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)  # one benchmark configuration at a time

jar = plan.get("jar", "/work/eval/target/pcc-eval.jar")
git_sha = subprocess.run(["git", "-c", "safe.directory=*", "-C", "/work", "rev-parse", "HEAD"],
                         stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout.strip()
dirty = subprocess.run(["git", "-c", "safe.directory=*", "-C", "/work", "status", "--porcelain", "--", "eval/src", "eval/scripts", "eval/pom.xml", "eval/docker", "src"],
                       stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout.strip()
if dirty:
    sys.exit("Refusing to run with uncommitted changes:\n" + dirty)
identity = dict(git_sha=git_sha, jar_sha256=sha256_file(jar), plan_sha256=sha256_file(plan_path))
identity_path = results / "identity.json"
if identity_path.exists():
    if json.load(open(identity_path)) != identity:
        sys.exit("Refusing to mix runs: this results directory holds another commit, jar or plan")
else:
    json.dump(identity, open(identity_path, "w"), indent=1)

spark_conf = plan.get("spark_conf", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "12"})
rng = random.Random(plan["seed"])
schedule = open(results / "schedule.jsonl", "a")
sequence = 0
only_reps = None if len(sys.argv) < 4 else {int(x) for x in sys.argv[3].split(",")}


def budget_from(cell, extra):
    extra = dict(extra)
    fraction = extra.pop("capacity-fraction", None)
    if fraction is not None:
        source = results / cell["fraction_of"] / "PCC_INLINE" / "rep0"
        if json.load(open(source / "run.json")).get("status") != "completed":
            raise ValueError(f"budget source {cell['fraction_of']} did not complete")
        rows = open(source / "queries.jsonl").read().strip().split("\n")
        extra["capacity-bytes"] = max(1, int(json.loads(rows[-1])["cache_bytes"] * fraction))
    return extra


for rep in range(max(cell.get("repeats", plan["repeats"]) for cell in plan["cells"])):
    for cell in plan["cells"]:
        if rep >= cell.get("repeats", plan["repeats"]):
            continue
        methods = cell.get("methods", plan["methods"])
        # always drawn, so a resumed or split run keeps the same orders
        order = rng.sample(methods, len(methods))
        if only_reps is not None and rep not in only_reps:
            continue
        for position, label in enumerate(order):
            sequence += 1
            out = results / cell["name"] / label / f"rep{rep}"
            if (out / "attempt.json").exists():
                continue  # already attempted: finished or failed, it stays as it is
            out.mkdir(parents=True, exist_ok=True)
            (out / "attempt.json").write_text(json.dumps(dict(sequence=sequence, started_epoch=round(time.time(), 1))))
            try:
                extra = budget_from(cell, cell.get("extra", {}))
            except (OSError, ValueError, IndexError) as e:
                record = dict(cell=cell["name"], method=label, rep=rep, position=position, sequence=sequence, exit_code=-1, skipped=str(e))
                schedule.write(json.dumps(record) + "\n")
                schedule.flush()
                print(json.dumps(record), flush=True)
                continue
            args = ["--method", label.split("@")[0], "--data", cell["layout"], "--trace", "file://" + os.path.abspath(cell["trace"]),
                    "--out", "file://" + str(out.resolve()), "--git-sha", git_sha, "--jar-sha256", identity["jar_sha256"],
                    "--plan-sha256", identity["plan_sha256"], "--cell", cell["name"], "--rep", str(rep),
                    "--position", str(position), "--sequence", str(sequence), "--label", label]
            for k, v in sorted(extra.items()):
                args += [f"--{k}", str(v)]
            cmd = ["timeout", "--kill-after=30", str(plan.get("timeout_s", 900)), "spark-submit", "--master", plan.get("master", "local[6]"), "--deploy-mode", "client",
                   "--driver-memory", plan.get("driver_memory", "8g")]
            for k, v in sorted(spark_conf.items()):
                cmd += ["--conf", f"{k}={v}"]
            cmd += ["--class", "cloud.ControlledRunner", jar] + args
            started = time.time()
            with open(out / "spark.log", "w") as log:
                code = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode
            record = dict(cell=cell["name"], method=label, rep=rep, position=position, sequence=sequence, exit_code=code,
                          started_epoch=round(started, 1), wall_s=round(time.time() - started, 1), extra=extra)
            schedule.write(json.dumps(record) + "\n")
            schedule.flush()
            print(json.dumps(record), flush=True)
