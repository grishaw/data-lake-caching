package local;

import org.apache.commons.lang3.tuple.Pair;
import org.tinspin.index.Index;
import org.tinspin.index.kdtree.KDIterator;
import org.tinspin.index.kdtree.KDTree;
import org.tinspin.index.phtree.PHTreeP;
import org.tinspin.index.qthypercube.QuadTreeKD;
import org.tinspin.index.rtree.RTree;
import org.tinspin.index.rtree.RTreeIterator;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static local.Benchmark.CacheType.*;

public class Benchmark {

    static final long DELAY_FOR_CLOUD_READ = 3000;
    static final int MIN_VALUE = - 1_000_000;
    static final int MAX_VALUE =  1_000_000;
    static final int UNLIMITED_CACHE_CAPACITY = -1;
    static final double MAX_FILES_FRACTION_FOR_CACHE = 0.7;

    public static void main(String[] args) {
        int numOfColumns = Integer.parseInt(args[0]);
        int numOfRecords = Integer.parseInt(args[1]);
        int numOfFiles = Integer.parseInt(args[2]);
        int numOfQueries = Integer.parseInt(args[3]);
        int checkpointNum = Integer.parseInt(args[4]);
        int cacheCapacity = Integer.parseInt(args[5]);
        double w1 = Double.parseDouble(args[6]);
        double w2 = Double.parseDouble(args[7]);
        CacheType cacheType =  CacheType.valueOf(args[8]);
        TestMode testMode =  TestMode.valueOf(args[8]);

        long start = System.currentTimeMillis();
        runBenchmark(numOfColumns, numOfRecords, numOfFiles, numOfQueries, checkpointNum, cacheCapacity, w1, w2, cacheType, testMode);
        long end = System.currentTimeMillis();

        System.out.println("benchmark took : " + (end-start) / 1000 / 60 + " minutes");
    }

    static void runBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int checkpointNum,
                             int cacheCapacity, double w1, double w2, CacheType cacheType, TestMode mode){

        if (mode == TestMode.SIMPLE) {

            int maxCoverageForCache = (int) Math.round(numOfFiles * MAX_FILES_FRACTION_FOR_CACHE);

            // baseline
            ArrayList<Long> listNoCacheTimes = new ArrayList<>();

            //TODO - add predicate caching (hashmap of interval and coverage)

            // our approach
            ArrayList<Long> listOurCacheTimes = new ArrayList<>();

            HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(numOfColumns, numOfRecords, numOfFiles);

            Cache cache = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, cacheType);

            int resultNoCache;
            int resultWithCache;

            for (int i = 1; i <= numOfQueries; i++) {

                ArrayList<Pair<Integer, Integer>> interval = generateInterval(numOfColumns, ThreadLocalRandom.current().nextDouble());

                long startNoCache = System.currentTimeMillis();
                resultNoCache = runQueryWithNoCache(table, interval);
                long endNoCache = System.currentTimeMillis();

                long startWithCache = System.currentTimeMillis();
                resultWithCache = runQueryWithCache(table, interval, cache);
                long endWithCache = System.currentTimeMillis();

                if (resultNoCache != resultWithCache) {
                    throw new IllegalStateException("results do not match !");
                }

                listNoCacheTimes.add(endNoCache - startNoCache);
                listOurCacheTimes.add(endWithCache - startWithCache);

                if (i > 0 && (i % checkpointNum == 0)) {
                    System.out.println("num of queries : " + i);
                    System.out.println();

                    Long noCacheAverage = (long) listNoCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                    Long noCacheTotal = listNoCacheTimes.stream().mapToLong(v -> v).sum();
                    System.out.println("no cache average : " + noCacheAverage);
                    System.out.println("no cache total : " + noCacheTotal);

                    System.out.println("------------------------");

                    Long withCacheAverage = (long) listOurCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                    Long withCacheTotal = listOurCacheTimes.stream().mapToLong(v -> v).sum();
                    Long hits = (long) (cache.hits.isEmpty() ? 0 : cache.hits.size());
                    System.out.println("with cache average : " + withCacheAverage);
                    System.out.println("with cache total : " + withCacheTotal);
                    System.out.println("total cache hits : " + hits);

                    System.out.println("****************************");
                    System.gc();
                }
            }
        }else if (mode == TestMode.CACHE_POLICY){
            cachePoliciesBenchmark(numOfColumns, numOfRecords, numOfFiles, numOfQueries, checkpointNum, cacheCapacity, cacheType);
        }else if (mode == TestMode.SPATIAL_INDEXES){
            spatialIndexBenchmark(numOfColumns, numOfRecords, numOfFiles, numOfQueries, checkpointNum, cacheCapacity);
        }
    }

    static void cachePoliciesBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int checkpointNum,
                                       int cacheCapacity, CacheType cacheType){

        int maxCoverageForCache = (int) Math.round(numOfFiles * MAX_FILES_FRACTION_FOR_CACHE);

        HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(numOfColumns, numOfRecords, numOfFiles);

        Cache cache1 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 1, 0, cacheType);
        Cache cache2 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 0, 1, cacheType);
        Cache cache3 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 0.5, 0.5, cacheType);
        Cache cache4 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 0.01, 0.99, cacheType);
        Cache cache5 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 0.99, 0.01, cacheType);
        Cache cache6 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 0.1, 0.9, cacheType);
        Cache cache7 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, 0.9, 0.1, cacheType);

        // baseline
        ArrayList<Long> listNoCacheTimes = new ArrayList<>();

        //TODO - add predicate caching (hashmap of interval and coverage)

        // our approach
        ArrayList<Long> listOurCacheTimes1 = new ArrayList<>(), listOurCacheTimes2 = new ArrayList<>(), listOurCacheTimes3 = new ArrayList<>(),
                listOurCacheTimes4 = new ArrayList<>(), listOurCacheTimes5 = new ArrayList<>(),
                listOurCacheTimes6 = new ArrayList<>(), listOurCacheTimes7 = new ArrayList<>();

        for (int i = 1; i <= numOfQueries; i++) {

            ArrayList<Pair<Integer, Integer>> interval = generateInterval(numOfColumns, ThreadLocalRandom.current().nextDouble());

            long start = System.currentTimeMillis();
            int resultNoCache = runQueryWithNoCache(table, interval);
            long end = System.currentTimeMillis();
            listNoCacheTimes.add(end - start);

            start = System.currentTimeMillis();
            int resultWithCache = runQueryWithCache(table, interval, cache1);
            end = System.currentTimeMillis();
            listOurCacheTimes1.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache2);
            end = System.currentTimeMillis();
            listOurCacheTimes2.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache3);
            end = System.currentTimeMillis();
            listOurCacheTimes3.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache4);
            end = System.currentTimeMillis();
            listOurCacheTimes4.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache5);
            end = System.currentTimeMillis();
            listOurCacheTimes5.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache6);
            end = System.currentTimeMillis();
            listOurCacheTimes6.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache7);
            end = System.currentTimeMillis();
            listOurCacheTimes7.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            if (i > 0 && (i % checkpointNum == 0)) {
                System.out.println("num of queries : " + i);
                System.out.println();

                Long noCacheAverage = (long) listNoCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                Long noCacheTotal = listNoCacheTimes.stream().mapToLong(v -> v).sum();
                System.out.println("no cache average : " + noCacheAverage);
                System.out.println("no cache total : " + noCacheTotal);

                System.out.println("------------------------");

                System.out.println(" cache 1 - w1 = 1, w2 = 0");
                Long withCacheAverage = (long) listOurCacheTimes1.stream().mapToLong(v -> v).average().getAsDouble();
                Long withCacheTotal = listOurCacheTimes1.stream().mapToLong(v -> v).sum();
                Long hits = (long) (cache1.hits.isEmpty() ? 0 : cache1.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 2 - w1 = 0, w2 = 1");
                withCacheAverage = (long) listOurCacheTimes2.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes2.stream().mapToLong(v -> v).sum();
                hits = (long) (cache2.hits.isEmpty() ? 0 : cache2.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 3 - w1 = 0.5, w2 = 0.5");
                withCacheAverage = (long) listOurCacheTimes3.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes3.stream().mapToLong(v -> v).sum();
                hits = (long) (cache3.hits.isEmpty() ? 0 : cache3.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 4 - w1 = 0.01, w2 = 0.99");
                withCacheAverage = (long) listOurCacheTimes4.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes4.stream().mapToLong(v -> v).sum();
                hits = (long) (cache4.hits.isEmpty() ? 0 : cache4.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 5 - w1 = 0.99, w2 = 0.01");
                withCacheAverage = (long) listOurCacheTimes5.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes5.stream().mapToLong(v -> v).sum();
                hits = (long) (cache5.hits.isEmpty() ? 0 : cache5.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 6 - w1 = 0.1, w2 = 0.9");
                withCacheAverage = (long) listOurCacheTimes6.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes6.stream().mapToLong(v -> v).sum();
                hits = (long) (cache6.hits.isEmpty() ? 0 : cache6.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 7 - w1 = 0.9, w2 = 0.1");
                withCacheAverage = (long) listOurCacheTimes7.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes7.stream().mapToLong(v -> v).sum();
                hits = (long) (cache7.hits.isEmpty() ? 0 : cache7.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println("****************************");
                System.gc();
            }
        }
    }

    static void spatialIndexBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int checkpointNum,
                                       int cacheCapacity){

        int maxCoverageForCache = (int) Math.round(numOfFiles * MAX_FILES_FRACTION_FOR_CACHE);

        HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(numOfColumns, numOfRecords, numOfFiles);

        double w1, w2;
        if (cacheCapacity != UNLIMITED_CACHE_CAPACITY){
            w1 = 1; w2 = 0;
        }else{
            w1 = w2 = 0;
        }

        Cache cache1 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, LINKED_LIST);
        Cache cache2 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, R_TREE);
        Cache cache3 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, QUAD_TREE);
        Cache cache4 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, KD_TREE);
        Cache cache5 = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, PH_TREE);

        // baseline
        ArrayList<Long> listNoCacheTimes = new ArrayList<>();

        //TODO - add predicate caching (hashmap of interval and coverage)

        // our approach
        ArrayList<Long> listOurCacheTimes1 = new ArrayList<>(), listOurCacheTimes2 = new ArrayList<>(), listOurCacheTimes3 = new ArrayList<>(),
                listOurCacheTimes4 = new ArrayList<>(), listOurCacheTimes5 = new ArrayList<>();

        for (int i = 1; i <= numOfQueries; i++) {

            ArrayList<Pair<Integer, Integer>> interval = generateInterval(numOfColumns, ThreadLocalRandom.current().nextDouble());

            long start = System.currentTimeMillis();
            int resultNoCache = runQueryWithNoCache(table, interval);
            long end = System.currentTimeMillis();
            listNoCacheTimes.add(end - start);

            start = System.currentTimeMillis();
            int resultWithCache = runQueryWithCache(table, interval, cache1);
            end = System.currentTimeMillis();
            listOurCacheTimes1.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache2);
            end = System.currentTimeMillis();
            listOurCacheTimes2.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache3);
            end = System.currentTimeMillis();
            listOurCacheTimes3.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache4);
            end = System.currentTimeMillis();
            listOurCacheTimes4.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            start = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cache5);
            end = System.currentTimeMillis();
            listOurCacheTimes5.add(end - start);

            if (resultNoCache != resultWithCache) {
                throw new IllegalStateException("results do not match !");
            }

            if (i > 0 && (i % checkpointNum == 0)) {
                System.out.println("num of queries : " + i);
                System.out.println();

                Long noCacheAverage = (long) listNoCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                Long noCacheTotal = listNoCacheTimes.stream().mapToLong(v -> v).sum();
                System.out.println("no cache average : " + noCacheAverage);
                System.out.println("no cache total : " + noCacheTotal);

                System.out.println("------------------------");

                System.out.println(" cache 1 - Linked List");
                Long withCacheAverage = (long) listOurCacheTimes1.stream().mapToLong(v -> v).average().getAsDouble();
                Long withCacheTotal = listOurCacheTimes1.stream().mapToLong(v -> v).sum();
                Long hits = (long) (cache1.hits.isEmpty() ? 0 : cache1.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 2 - R-Tree");
                withCacheAverage = (long) listOurCacheTimes2.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes2.stream().mapToLong(v -> v).sum();
                hits = (long) (cache2.hits.isEmpty() ? 0 : cache2.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 3 - Quad-Tree");
                withCacheAverage = (long) listOurCacheTimes3.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes3.stream().mapToLong(v -> v).sum();
                hits = (long) (cache3.hits.isEmpty() ? 0 : cache3.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 4 - KD-TRee");
                withCacheAverage = (long) listOurCacheTimes4.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes4.stream().mapToLong(v -> v).sum();
                hits = (long) (cache4.hits.isEmpty() ? 0 : cache4.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println(" cache 5 - PH-Tree");
                withCacheAverage = (long) listOurCacheTimes5.stream().mapToLong(v -> v).average().getAsDouble();
                withCacheTotal = listOurCacheTimes5.stream().mapToLong(v -> v).sum();
                hits = (long) (cache5.hits.isEmpty() ? 0 : cache5.hits.size());
                System.out.println("with cache average : " + withCacheAverage);
                System.out.println("with cache total : " + withCacheTotal);
                System.out.println("total cache hits : " + hits);

                System.out.println("****************************");
                System.gc();
            }
        }
    }

    // table
    static HashMap<Integer, List<ArrayList<Integer>>> createRandomTable(int numOfColumns, int numOfRecords, int numOfFiles){

        HashMap<Integer, List<ArrayList<Integer>>> result = new HashMap<>();

        for (int i=0; i<numOfRecords; i++){
            ArrayList<Integer> curRecord = generateRecord(numOfColumns);
            int curPartition = ThreadLocalRandom.current().nextInt(numOfFiles);
            result.computeIfAbsent(curPartition, x -> new LinkedList<>()).add(curRecord);
        }

        return result;
    }

    static ArrayList<Integer> generateRecord(int numOfColumns){
        ArrayList<Integer> result = new ArrayList<>(numOfColumns);

        for (int i=0; i<numOfColumns; i++){
            result.add(ThreadLocalRandom.current().nextInt(MIN_VALUE, MAX_VALUE));
        }

        return result;
    }

    // queries
    static int runQueryWithCache(HashMap<Integer, List<ArrayList<Integer>>> table, ArrayList<Pair<Integer, Integer>> interval, Cache cache){
        Set<Integer> coverage = new HashSet<>();
        Set<Integer> minCoverage = cache.getMinCoverage(interval);

        int resultCount = 0;
        if (minCoverage == null) {
            for (int fileNum : table.keySet()) {
                readFileFromCloudSimulation();
                List<ArrayList<Integer>> records = table.get(fileNum);
                for (ArrayList<Integer> record : records) {
                    if (Benchmark.intervalContainsRecord(interval, record)) {
                        resultCount++;
                        coverage.add(fileNum);
                    }
                }
            }
        }else{
            for (int fileNum : minCoverage){
                readFileFromCloudSimulation();
                for (ArrayList<Integer> record : table.get(fileNum)) {
                    if (Benchmark.intervalContainsRecord(interval, record)) {
                        resultCount++;
                        coverage.add(fileNum);
                    }
                }
            }
        }

        if (coverage.size() < cache.maxCoverage && (minCoverage == null || minCoverage.size() > coverage.size())){
            cache.put(interval, coverage);
        }

        return resultCount;
    }

    static int runQueryWithNoCache(HashMap<Integer, List<ArrayList<Integer>>> table, ArrayList<Pair<Integer, Integer>> interval){

        int resultCount = 0;

        for (int fileNum : table.keySet()) {
            List<ArrayList<Integer>> records = table.get(fileNum);
            readFileFromCloudSimulation();
            for (ArrayList<Integer> record : records) {
                if (Benchmark.intervalContainsRecord(interval, record)) {
                    resultCount++;
                }
            }
        }

        return resultCount;
    }

    // intervals
    static ArrayList<Pair<Integer, Integer>> generateInterval (int totalTerms, double percentageOfNonEmptyTerms){
        ArrayList<Pair<Integer, Integer>> result = new ArrayList<>(totalTerms);

        for (int i=0; i < totalTerms; i++){

            double randomValue =  ThreadLocalRandom.current().nextDouble();

            // non-empty case - generate random term
            if (randomValue <= percentageOfNonEmptyTerms){
                int random1 = ThreadLocalRandom.current().nextInt(MIN_VALUE, MAX_VALUE);
                int random2 = ThreadLocalRandom.current().nextInt(MIN_VALUE, MAX_VALUE);

                result.add(Pair.of(Math.min(random1, random2), Math.max(random1, random2)));
            }else{
                // no term - put min/max
                result.add(Pair.of(MIN_VALUE, MAX_VALUE));
            }
        }

        return result;
    }

    static boolean intervalContainsRecord(ArrayList<Pair<Integer, Integer>> interval, ArrayList<Integer> record){
        if (interval.size() != record.size()){
            throw new IllegalArgumentException("interval and record should be of the same size!");
        }

        for (int i=0; i<interval.size(); i++){
            if (record.get(i) < interval.get(i).getLeft() || record.get(i) > interval.get(i).getRight()){
                return false;
            }
        }

        return true;

    }

    static boolean intervalContainsInterval(ArrayList<Pair<Integer, Integer>> interval1, ArrayList<Pair<Integer, Integer>> interval2){
        if (interval1.size() != interval2.size()){
            throw new IllegalArgumentException("intervals should be of the same size!");
        }

        for (int i=0; i<interval1.size(); i++){
            if (interval2.get(i).getLeft() < interval1.get(i).getLeft() || interval2.get(i).getRight() > interval1.get(i).getRight()){
                return false;
            }
        }

        return true;

    }

    static void readFileFromCloudSimulation(){
        long start = System.nanoTime();
        while(start +  DELAY_FOR_CLOUD_READ >= System.nanoTime());
    }

    // cache
    static class Cache {
        ArrayList<Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>>> cache;
        List<Integer> hits;

        PriorityQueue<Double> heap;

        int maxCoverage;
        int capacity;

        double w1;
        double w2;

        double maxVolume;

        Index cacheSpatialIndex;

        CacheType cacheType;

        public Cache(int maxCoverage, int capacity, int numOfColumns, double w1, double w2, CacheType cacheType){
            this.maxCoverage = maxCoverage;
            this.capacity = capacity;
            this.cacheType = cacheType;

            if (cacheType == CacheType.LINKED_LIST) {
                cache = new ArrayList<>();
            }else{
                if (cacheType == R_TREE){
                    cacheSpatialIndex = RTree.createRStar(numOfColumns * 2);
                } else if (cacheType == KD_TREE) {
                    cacheSpatialIndex = KDTree.create(numOfColumns * 2);
                } else if (cacheType == QUAD_TREE){
                    cacheSpatialIndex = QuadTreeKD.create(numOfColumns * 2);
                } else if (cacheType == PH_TREE){
                    cacheSpatialIndex = PHTreeP.create(numOfColumns * 2);
                }
            }
            hits = new LinkedList<>();
            heap = new PriorityQueue<>();
            maxVolume = getMaxVolume(numOfColumns);
            this.w1 = w1;
            this.w2 = w2;

            if (capacity != UNLIMITED_CACHE_CAPACITY && (w1 + w2 != 1)){
                throw new IllegalArgumentException("w1 + w1 must be 1");
            }
        }

        Set<Integer> getMinCoverage (ArrayList<Pair<Integer, Integer>> queryInterval){
            if (cacheType == R_TREE || cacheType == KD_TREE || cacheType == QUAD_TREE || cacheType == PH_TREE){
                double [] queryMin = getQueryMin (queryInterval.size() * 2);
                double [] queryMax = mapIntervalToPoint(queryInterval);

                if (queryMin.length != queryMax.length){
                    throw new IllegalStateException("lengths do not match");
                }

                Set<Integer> result = null;
                Iterator <Set<Integer>> it = null;

                if (cacheType == R_TREE) {
                   it = ((RTree) cacheSpatialIndex).queryIntersect(queryMin, queryMax);
                }else if (cacheType == KD_TREE){
                    it = ((KDTree) cacheSpatialIndex).query(queryMin, queryMax);
                }else if (cacheType == QUAD_TREE){
                    it = ((QuadTreeKD) cacheSpatialIndex).query(queryMin, queryMax);
                }else if (cacheType == PH_TREE){
                    it = ((PHTreeP) cacheSpatialIndex).query(queryMin, queryMax);
                }

                while (it.hasNext()){
                    Set<Integer> curSet = null;
                    if (cacheType == R_TREE) {
                        curSet = (Set<Integer>)((RTreeIterator)it).next().value();
                    }else if (cacheType == KD_TREE){
                        curSet = (Set<Integer>)((KDIterator)it).next().value();
                    }else if (cacheType == QUAD_TREE){
                        curSet = ((Index.PointEntry<Set<Integer>>)((Index.PointIterator)it).next()).value();
                    }else if (cacheType == PH_TREE){
                        curSet = ((Index.PointEntry<Set<Integer>>)((Index.PointIterator)it).next()).value();
                    }

                    if (result == null || result.size() > curSet.size()){
                        result = curSet;
                    }
                }
                if (result != null) {
                    hits.add(result.size());
                }
                return result;
            }else {
                Set<Integer> result = null;
                for (Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>> entry : cache) {
                    if (intervalContainsInterval(entry.getLeft(), queryInterval) && (result == null || result.size() > entry.getRight().size())) {
                        result = entry.getRight();
                    }
                }

                if (result != null) {
                    hits.add(result.size());
                }

                return result;
            }
        }

        void put(ArrayList<Pair<Integer, Integer>> interval, Set<Integer> coverage){

            if (coverage.size() < maxCoverage){

                // unlimited
                if (capacity == UNLIMITED_CACHE_CAPACITY){
                    if (this.cacheType == R_TREE) {
                        ((RTree) cacheSpatialIndex).insert(mapIntervalToPoint(interval), coverage);
                    } else if (cacheType == KD_TREE) {
                        ((KDTree) cacheSpatialIndex).insert(mapIntervalToPoint(interval), coverage);
                    } else if (cacheType == QUAD_TREE) {
                        ((QuadTreeKD) cacheSpatialIndex).insert(mapIntervalToPoint(interval), coverage);
                    } else if (cacheType == PH_TREE) {
                        ((PHTreeP) cacheSpatialIndex).insert(mapIntervalToPoint(interval), coverage);
                    }
                    else {
                        cache.add(Pair.of(interval, coverage));
                    }
                } else {

                    double gFunctionResult = gFunction(interval, coverage.size());

                    if (cache.size() > capacity) {
                        Double toRemove = heap.poll();
                        if (toRemove != null) {
                            for (Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>> cur : cache) {
                                if (gFunction(cur.getLeft(), cur.getRight().size()) == toRemove){
                                    cache.remove(cur);
                                    break;
                                }
                            }
                        }
                    }

                    heap.add(gFunctionResult);
                    cache.add(Pair.of(interval, coverage));
                }

            }
        }

        double gFunction(ArrayList<Pair<Integer, Integer>> interval, int coverageSize){
            double intervalVolume = intervalVolume(interval);

            return w1 * (intervalVolume / maxVolume ) +
                    w2 * (1 - ( (coverageSize / (1.0 * maxCoverage)) ));
        }
    }

    static double intervalVolume(ArrayList<Pair<Integer, Integer>> interval){
        return interval.stream().
                mapToInt(p -> p.getRight() - p.getLeft())
                .mapToDouble(x -> Math.log(x))
                .sum();
    }

    static double getMaxVolume(int numOfCols){
        ArrayList<Pair<Integer, Integer>> interval = new ArrayList<>(numOfCols);

        for (int i=0; i< numOfCols; i++){
            interval.add(Pair.of(MIN_VALUE, MAX_VALUE));
        }

        return intervalVolume(interval);
    }

    enum CacheType{
        LINKED_LIST,
        R_TREE,
        KD_TREE,
        QUAD_TREE,
        PH_TREE
    }

    static double[] mapIntervalToPoint(ArrayList<Pair<Integer, Integer>> interval){
        double [] result = new double[interval.size() * 2];

        for (int i=0; i< interval.size(); i++){
            result[i] = interval.get(i).getLeft();
        }

        for (int i=0; i< interval.size(); i++){
            result[interval.size() + i] = (-1) * interval.get(i).getRight();
        }

        return result;
    }

    static double [] getQueryMin(int numOfColumns){
        double [] result = new double[numOfColumns];
        Arrays.fill(result, MIN_VALUE);
        return result;
    }

    enum TestMode{
        SIMPLE,
        CACHE_POLICY,
        SPATIAL_INDEXES
    }
}
