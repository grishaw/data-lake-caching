#!/usr/bin/env python3
"""Independent answers for a trace: count, exact sum and coverage per query, computed with pyarrow and integer arithmetic.

Usage: oracle.py <layout dir> <trace.jsonl> <out.jsonl>
"""
import datetime as dt
import glob
import hashlib
import json
import os
import sys
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

layout, trace_path, out = sys.argv[1], sys.argv[2], sys.argv[3]
files = sorted(glob.glob(f"{layout}/*.parquet"))
columns = []
for name in files:
    t = pq.read_table(name, columns=["l_shipdate", "l_discount", "l_quantity", "l_extendedprice", "l_returnflag", "l_linestatus"])
    days = t["l_shipdate"].cast(pa.int32()).to_numpy()
    cents = lambda c: np.array(pc.multiply(pc.cast(t[c], pa.float64()), 100.0).to_numpy().round(), dtype=np.int64)
    flags = np.char.add(t["l_returnflag"].to_numpy(zero_copy_only=False).astype(str), t["l_linestatus"].to_numpy(zero_copy_only=False).astype(str))
    columns.append((os.path.basename(name), days, cents("l_discount"), cents("l_quantity"), cents("l_extendedprice"), flags))
EPOCH = dt.date(1970, 1, 1)


def as_cents_floor(x):
    return int(np.floor(round(x * 100, 9)))


def as_cents_ceil(x):
    return int(np.ceil(round(x * 100, 9)))


os.makedirs(os.path.dirname(out), exist_ok=True)
with open(trace_path) as src, open(out, "w") as dst:
    for line in src:
        q = json.loads(line)
        lo = (dt.date.fromisoformat(q["ship_from"]) - EPOCH).days
        hi = (dt.date.fromisoformat(q["ship_to"]) - EPOCH).days
        # discount bounds may be arbitrary doubles while the column holds whole cents
        dlo, dhi = as_cents_ceil(q["disc_from"]), as_cents_floor(q["disc_to"])
        count, total, cover, groups = 0, 0, [], {}
        for name, days, disc, qty, price, flags in columns:
            mask = (days >= lo) & (days <= hi) & (disc >= dlo) & (disc <= dhi) \
                & (qty >= q["qty_from"] * 100) & (qty <= q["qty_to"] * 100)
            n = int(mask.sum())
            if n:
                count += n
                total += int((price[mask] * disc[mask]).sum())
                cover.append(name)
                if q.get("template") == "q1":
                    for g in np.unique(flags[mask]):
                        m2 = mask & (flags == g)
                        old = groups.get(str(g), (0, 0))
                        groups[str(g)] = (old[0] + int(m2.sum()), old[1] + int((price[m2] * disc[m2]).sum()))
        sum_text = None if count == 0 else str(Decimal(total) / Decimal(10000))
        digest = hashlib.sha256("\n".join(sorted(cover)).encode()).hexdigest()
        group_text = {g: [n, str(Decimal(v) / Decimal(10000))] for g, (n, v) in groups.items()}
        dst.write(json.dumps(dict(seq=q["seq"], count=count, sum=sum_text, groups=group_text, coverage=cover, coverage_sha256=digest)) + "\n")
