#!/usr/bin/env bash
# Everything local, in order, one clean batch. Run from a clean commit.
cd /work/eval
python3 scripts/gen_traces.py
python3 scripts/gen_plans.py
mvn -q -B -DskipTests package > /dev/null 2>&1 || { echo "build failed"; exit 1; }
python3 scripts/run_matrix.py plans/local.json results/local > results/local_matrix.log 2>&1; echo "matrix exit $?"
python3 scripts/analyze.py results/local plans/local.json > results/local_analyze.log 2>&1; echo "analyze exit $?"
python3 scripts/run_matrix.py plans/local_extra.json results/local_extra_quiet > results/local_extra_quiet_matrix.log 2>&1; echo "extra matrix exit $?"
python3 scripts/analyze.py results/local_extra_quiet plans/local_extra.json > results/local_extra_quiet_analyze.log 2>&1; echo "extra analyze exit $?"
python3 scripts/run_matrix.py plans/local_table3.json results/local_table3 > /dev/null 2>&1; python3 scripts/analyze.py results/local_table3 plans/local_table3.json > /dev/null 2>&1; echo "Table 3 path exit $?"
python3 scripts/run_matrix.py plans/local_aa_skew.json results/local_aa_skew > /dev/null 2>&1; python3 scripts/analyze.py results/local_aa_skew plans/local_aa_skew.json > /dev/null 2>&1; echo "skew A/A exit $?"
