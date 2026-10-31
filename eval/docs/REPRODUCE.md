# Reproducing the local checks

Everything runs in one container with fixed limits (6 CPU, 12 GB). One benchmark configuration runs at a time.
The local runs check the method. Their numbers say nothing about cloud performance.

## Setup

From the repo root:

```bash
git worktree add ../pcc-eval-original 581036c
colima start pcc --cpu 8 --memory 16 --disk 80 --runtime docker
export DOCKER_HOST=unix://$HOME/.colima/pcc/docker.sock
cd eval
scripts/fetch_spark.sh
docker compose --profile lab build
docker compose --profile lab up -d
docker compose exec -T lab bash scripts/build.sh
```

## Data

SF1, then SF10 for the unchanged original, which cannot finish at SF1:

```bash
docker compose exec -T lab bash -c 'spark-submit --master "local[6]" --driver-memory 8g --files /opt/tpch-dbgen/dbgen,/opt/tpch-dbgen/dists.dss --class cloud.PrepareData target/pcc-eval.jar 1 1 /work/eval/data/sf1/stage file:///work/eval/data/sf1 random:100,sorted:100,skewqty:100'
docker compose exec -T lab bash -c 'spark-submit --master "local[6]" --driver-memory 8g --files /opt/tpch-dbgen/dbgen,/opt/tpch-dbgen/dists.dss --class cloud.PrepareData target/pcc-eval.jar 10 6 /work/eval/data/sf10/stage file:///work/eval/data/sf10 random:1000'
```

## Stage A: unchanged original

```bash
docker compose exec -T lab bash scripts/run_original.sh data/sf1/random_100 results/stage_a_original/sf1_q10 10 50 1 50 1
docker compose exec -T lab bash scripts/run_original.sh data/sf1/random_100 results/stage_a_original/sf1_q50 50 50 1 50 1
docker compose exec -T lab bash scripts/run_original.sh data/sf10/random_1000 results/stage_a_original/sf10_q10 10 500 1 50 1
```

The two SF1 runs are expected to fail with a null error on an empty sum. The SF10 run may too, because the query generator is unseeded.

## Everything else, in one clean batch

Run from a clean commit. This generates the workloads and plans, then runs the matrix and the answer checks:

```bash
docker compose exec -T lab bash scripts/local_all.sh
```

`run_matrix.py` refuses a results directory from another commit, jar or plan. Move the old directory aside to rerun.

Held-out check, run once, after everything is frozen:

```bash
docker compose exec -T lab python3 scripts/run_matrix.py plans/local_heldout.json results/local_heldout
docker compose exec -T lab python3 scripts/analyze.py results/local_heldout plans/local_heldout.json
```

## Stop

```bash
docker compose --profile lab down
colima stop pcc
```
