#!/usr/bin/env python3
"""Writes every saved trace from fixed seeds. Re-running reproduces identical files."""
import datetime as dt
import json
import pathlib
import random
import sys

SHIP_MIN = dt.date(1992, 1, 2)
SHIP_MAX = dt.date(1998, 12, 1)


def q(klass, ship_from, ship_to, disc_from, disc_to, qty_from, qty_to):
    return dict(klass=klass, ship_from=str(ship_from), ship_to=str(ship_to), disc_from=disc_from,
                disc_to=disc_to, qty_from=qty_from, qty_to=qty_to)


def rand_date(r, lo=SHIP_MIN, hi=SHIP_MAX):
    return lo + dt.timedelta(days=r.randrange((hi - lo).days + 1))


def paper_dec1998(r, n):
    """Distribution of the README commands: quantityToMax=50, discountToMax=1, Dec 1998 dates."""
    out = []
    for _ in range(n):
        ship_to = dt.date(1998, 12, 1) + dt.timedelta(days=r.randrange(30))
        out.append(q("paper_dec1998", dt.date(1998, 12, 1), ship_to, 0.0, r.uniform(0.0, 1.0), 1, r.randrange(1, 50)))
    return out


def official_q6(r, n):
    """TPC-H 3.0.1 clause 2.4.6.3 substitution ranges, written as closed intervals."""
    out = []
    for _ in range(n):
        year = r.randrange(1993, 1998)
        discount = r.randrange(2, 10) / 100
        quantity = r.randrange(24, 26)
        out.append(q("official_q6", dt.date(year, 1, 1), dt.date(year, 12, 31),
                     round(discount - 0.01, 2), round(discount + 0.01, 2), 1, quantity - 1))
    return out


def official_q1(r, n):
    """TPC-H 3.0.1 Q1: shipdate <= 1998-12-01 minus DELTA days, DELTA in [60, 120]."""
    return [dict(q("official_q1", SHIP_MIN, dt.date(1998, 12, 1) - dt.timedelta(days=r.randrange(60, 121)), 0.0, 0.1, 1, 50),
                 template="q1") for _ in range(n)]


def narrow(r):
    """A selective predicate: a few days, a few discounts, a few quantities."""
    start = rand_date(r, SHIP_MIN, SHIP_MAX - dt.timedelta(days=60))
    days = r.choice([1, 3, 7, 14, 30])
    dlo = r.randrange(0, 9) / 100
    qlo = r.randrange(1, 40)
    return q("fresh", start, start + dt.timedelta(days=days), dlo, round(dlo + r.choice([0.01, 0.02]), 2),
             qlo, qlo + r.choice([2, 5, 10]))


def shrink(r, base):
    """A strict sub-interval of base in every dimension that has room."""
    lo, hi = dt.date.fromisoformat(base["ship_from"]), dt.date.fromisoformat(base["ship_to"])
    span = (hi - lo).days
    a = r.randrange(0, max(1, span // 3 + 1))
    b = r.randrange(0, max(1, span // 3 + 1))
    new_hi = max(lo + dt.timedelta(days=a), hi - dt.timedelta(days=b))
    disc_hi = base["disc_to"]
    if disc_hi - base["disc_from"] >= 0.01 and r.random() < 0.5:
        disc_hi = round(disc_hi - 0.01, 2)
    return q("contained", lo + dt.timedelta(days=a), new_hi, base["disc_from"], disc_hi,
             base["qty_from"] + (1 if base["qty_to"] > base["qty_from"] else 0), base["qty_to"])


def shift(r, base):
    """Overlaps base but is not contained in it: the end date moves past base."""
    lo, hi = dt.date.fromisoformat(base["ship_from"]), dt.date.fromisoformat(base["ship_to"])
    span = max(1, (hi - lo).days)
    delta = dt.timedelta(days=r.randrange(span // 2 + 1, span + 1))
    return q("overlap", lo + delta, hi + delta, base["disc_from"], base["disc_to"], base["qty_from"], base["qty_to"])


def mix(r, n, repeat=0.2, contained=0.3, overlap=0.2, fresh=0.2):
    """Each class at a stated share, drawn in random order after a short seeding prefix. The rest are empty queries."""
    out = [narrow(r) for _ in range(5)]
    while len(out) < n:
        roll = r.random()
        seeds = [x for x in out if x["klass"] == "fresh"]
        base = r.choice(seeds)
        if roll < repeat:
            out.append(dict(base, klass="exact_repeat"))
        elif roll < repeat + contained:
            out.append(shrink(r, base))
        elif roll < repeat + contained + overlap:
            out.append(shift(r, base))
        elif roll < repeat + contained + overlap + fresh:
            out.append(narrow(r))
        else:
            # past the last ship date, so it must be empty
            start = dt.date(1998, 12, 2) + dt.timedelta(days=r.randrange(20))
            out.append(q("empty", start, start + dt.timedelta(days=5), 0.0, 0.1, 1, 50))
    return out


def zipf(r, n, pool_size=30, s=1.1, repeat=0.5):
    """Zipfian popularity over a pool. Each draw is an exact repeat with this share, otherwise a narrowed variant."""
    pool = [narrow(r) for _ in range(pool_size)]
    weights = [1 / (i + 1) ** s for i in range(pool_size)]
    out = []
    for _ in range(n):
        base = r.choices(pool, weights=weights)[0]
        out.append(dict(base, klass="zipf_repeat") if r.random() < repeat else dict(shrink(r, base), klass="zipf_narrowed"))
    return out


def zipf_contained(r, n):
    """Popular regions, never the same literals twice, so only containment can hit."""
    return zipf(r, n, repeat=0.0)


def zipf_q1(r, n):
    """The Q1 template with selective predicates, because official Q1 covers nearly every file."""
    return [dict(x, template="q1") for x in zipf(r, n, repeat=0.0)]


def share(repeat, contained):
    def build(r, n):
        return mix(r, n, repeat=repeat, contained=contained, overlap=0.1, fresh=0.9 - repeat - contained)
    return build


def paper_rounded(r, n):
    """The paper's generator with bounds clipped to the data: whole-cent discounts capped at 0.10, so equal predicates compare equal."""
    return [dict(x, klass="paper_rounded", disc_to=min(0.10, round(x["disc_to"] + 0.005, 2)), ship_to="1998-12-01") for x in paper_dec1998(r, n)]


def paper_test_ranges(r, n):
    """The ranges in the repo's BenchmarkTest defaults, quantityToMax=3, discountToMax=0.03."""
    out = []
    for _ in range(n):
        ship_to = dt.date(1998, 12, 1) + dt.timedelta(days=r.randrange(30))
        out.append(q("paper_test_ranges", dt.date(1998, 12, 1), ship_to, 0.0, r.uniform(0.0, 0.03), 1, r.randrange(1, 3)))
    return out


# name -> (seed, builder, query count). Seeds are fixed before any result exists.
TRACES = {
    "smoke_paper_dec1998": (1001, paper_dec1998, 50),
    "smoke_paper_dec1998_s2": (1011, paper_dec1998, 50),
    "smoke_paper_dec1998_s3": (1021, paper_dec1998, 50),
    "smoke_official_q6": (1002, official_q6, 50),
    "smoke_zipf": (1004, zipf, 50),
    "smoke_official_q1": (1005, official_q1, 20),
    "smoke_zipf_contained": (1006, zipf_contained, 50),
    "smoke_zipf_q1": (1007, zipf_q1, 30),
    "smoke_share_repeat0": (1031, share(0.0, 0.6), 50),
    "smoke_share_repeat30": (1032, share(0.3, 0.3), 50),
    "smoke_share_repeat60": (1033, share(0.6, 0.0), 50),
    "smoke_paper_rounded": (1041, paper_rounded, 50),
    "smoke_paper_test_ranges": (1042, paper_test_ranges, 50),
    "paper_table3": (1051, paper_dec1998, 100),
    "heldout_mix": (9101, mix, 50),
}

if __name__ == "__main__":
    OUT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "traces")
    OUT.mkdir(parents=True, exist_ok=True)
    for name, (seed, build, n) in TRACES.items():
        rng = random.Random(seed)
        with open(OUT / f"{name}.jsonl", "w") as f:
            for i, row in enumerate(build(rng, n)):
                f.write(json.dumps(dict(seq=i, **row), sort_keys=True) + "\n")
    print("wrote", len(TRACES), "traces to", OUT)
