# Predicate Containment Caching in Cloud Data Lakes

This is a proof-of-concept implementation of the ideas presented in the "Predicate Containment Caching in Cloud Data Lakes" paper.

## Cloud Tests

Cloud tests ran on a 100GB TPCH lineitem table stored in S3 in Parquet Format.
The experiments were performed on an AWS EMR cluster (6.10.1) with
10 nodes of m6g.2xlarge type (each with 8 vCore, 30.5 GB
memory, 64 GB EBS storage).

The following spark-submit commands were executed with the below bottom-line results.

10k
-------

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_10k_parquet/lineitem100_parquet/ 10 5000 3 50 1

Bottom line: cache = 48, non-cache = 60, naive-cache = 60

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_10k_parquet/lineitem100_parquet/ 50 5000 3 50 1

Bottom line: cache = 185, non-cache = 305, naive-cache = 317

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_10k_parquet/lineitem100_parquet/ 100 5000 3 50 1

Bottom line: cache = 341, non-cache = 607, naive-cache = 611

20k
-----
spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_20k_parquet/ 10 10000 3 50 1

Bottom line: cache = 68, non-cache = 90, naive-cache = 90

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_20k_parquet/ 50 10000 3 50 1

Bottom line: cache = 258, non-cache = 456, naive-cache = 456

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_20k_parquet/ 100 10000 3 50 1

Bottom line: cache = 391, non-cache = 910, naive-cache = 914


50k
-----
spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_50k_parquet/ 10 25000 3 50 1

Bottom line: cache = 154, non-cache = 207, naive-cache = 203

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_50k_parquet/ 50 25000 3 50 1

Bottom line: cache = 453, non-cache = 1030, naive-cache = 1018

spark-submit --deploy-mode cluster --class cloud.Benchmark s3://my-bucket/jars/2026-01-17-caching.jar s3://my-bucket/tpch/lineitem100_50k_parquet/ 100 25000 3 50 1

Bottom line: cache = 681, non-cache = 2056, naive-cache = 2067
