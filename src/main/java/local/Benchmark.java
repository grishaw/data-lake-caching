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

        long start = System.currentTimeMillis();
        runBenchmark(numOfColumns, numOfRecords, numOfFiles, numOfQueries, checkpointNum, cacheCapacity, w1, w2);
        long end = System.currentTimeMillis();

        System.out.println("benchmark took : " + (end-start) / 1000 / 60 + " minutes");
    }

    static void runBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int checkpointNum, int cacheCapacity, double w1, double w2){

        //TODO
        // - organize the flow to be simple and readable
        // - add relevant classes - for table, interval, record (don't use apache pair)
        // - try different params
        // - add cache data structure type (basic/enhanced/spatial type) and cache replacement type (unlimited, policy type)
        // - add missing tests / refactor existing
        // - add ReadMe, squash commits
        // - consider hashmap for cache instead of list (to avoid duplicate intervals)
        // - consider running on AWS

        ArrayList<Long> listNoCacheTimes = new ArrayList<>();
        ArrayList<Long> listNoCacheTimesSummary = new ArrayList<>();

        ArrayList<Long> listWithCacheTimes = new ArrayList<>();
        ArrayList<Long> listWithCacheTimesSummary = new ArrayList<>();

        ArrayList<Long> listWithCacheRemoveMinTimes = new ArrayList<>();
        ArrayList<Long> listWithCacheRemoveMinTimesSummary = new ArrayList<>();

        ArrayList<Long> listWithRTreeCacheTimes = new ArrayList<>();
        ArrayList<Long> listWithRTreeCacheTimesSummary = new ArrayList<>();

        ArrayList<Integer> cacheHitsSummary = new ArrayList<>();
        ArrayList<Integer> cacheRemoveMinHitsSummary = new ArrayList<>();
        ArrayList<Integer> cacheRTreeHitsSummary = new ArrayList<>();

        ArrayList<Long> hitsCoverageAverageSummary = new ArrayList<>();
        ArrayList<Long> hitsRemoveMinCoverageAverageSummary = new ArrayList<>();
        ArrayList<Long> hitsRTreeAverageSummary = new ArrayList<>();

        HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(numOfColumns, numOfRecords, numOfFiles);

        int maxCoverageForCache = (int) Math.round(numOfFiles * MAX_FILES_FRACTION_FOR_CACHE);
        Cache cacheUnlimited = new Cache(maxCoverageForCache, UNLIMITED_CACHE_CAPACITY, numOfColumns, w1, w2, LINKED_LIST);
        Cache cacheRemoveMin = new Cache(maxCoverageForCache, cacheCapacity, numOfColumns, w1, w2, LINKED_LIST);
        Cache cacheRTree = new Cache(maxCoverageForCache, UNLIMITED_CACHE_CAPACITY, numOfColumns, w1, w2, KD_TREE);

        int resultNoCache;
        int resultWithCache;
        int resultWithCacheRemoveMin;
        int resultWithRTreeCache;

        for (int i = 1; i <= numOfQueries; i++) {

            ArrayList<Pair<Integer, Integer>> interval = generateInterval(numOfColumns, ThreadLocalRandom.current().nextDouble());

            long startNoCache = System.currentTimeMillis();
            resultNoCache = runQueryWithNoCache(table, interval);
            long endNoCache = System.currentTimeMillis();

            long startWithCache = System.currentTimeMillis();
            resultWithCache = runQueryWithCache(table, interval, cacheUnlimited);
            long endWithCache = System.currentTimeMillis();

            long startWithCacheRemoveMin = System.currentTimeMillis();
            resultWithCacheRemoveMin = runQueryWithCache(table, interval, cacheRemoveMin);
            long endWithCacheRemoveMin = System.currentTimeMillis();

            long startWithRTreeCache = System.currentTimeMillis();
            resultWithRTreeCache = runQueryWithCache(table, interval, cacheRTree);
            long endWithRTreeCache = System.currentTimeMillis();


            if (resultNoCache != resultWithCache || resultWithCache !=  resultWithCacheRemoveMin || resultWithCacheRemoveMin != resultWithRTreeCache){
                throw new IllegalStateException("results do not match : " + resultNoCache + ", " + resultWithCache + ", " + resultWithCacheRemoveMin + ", " + resultWithRTreeCache);
            }

            listNoCacheTimes.add(endNoCache - startNoCache);
            listWithCacheTimes.add(endWithCache - startWithCache);
            listWithCacheRemoveMinTimes.add(endWithCacheRemoveMin - startWithCacheRemoveMin);
            listWithRTreeCacheTimes.add(endWithRTreeCache - startWithRTreeCache);

            System.out.println(i + " done");

            if (i>0 && (i % checkpointNum == 0)){
                System.out.println("num of queries : " + i);

                Long noCacheAverage = (long) listNoCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                System.out.println("no cache average : " + noCacheAverage);
                listNoCacheTimesSummary.add(noCacheAverage);

                Long withCacheAverage = (long) listWithCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                System.out.println("with cache average : " + withCacheAverage);
                listWithCacheTimesSummary.add(withCacheAverage);

                Long withCacheRemoveMinAverage = (long) listWithCacheRemoveMinTimes.stream().mapToLong(v -> v).average().getAsDouble();
                System.out.println("with cache remove min average : " + withCacheRemoveMinAverage);
                listWithCacheRemoveMinTimesSummary.add(withCacheRemoveMinAverage);

                Long withCacheRTreeAverage = (long) listWithRTreeCacheTimes.stream().mapToLong(v -> v).average().getAsDouble();
                System.out.println("with cache RTree average : " + withCacheRTreeAverage);
                listWithRTreeCacheTimesSummary.add(withCacheRTreeAverage);

                Integer hits = cacheUnlimited.hits.isEmpty() ? null : cacheUnlimited.hits.size();
                System.out.println("hits for unlimited cache total = " + hits);
                cacheHitsSummary.add(hits);

                hits = cacheRemoveMin.hits.isEmpty() ? null : cacheRemoveMin.hits.size();
                System.out.println("hits for limited cache total = " + hits);
                cacheRemoveMinHitsSummary.add(hits);

                hits = cacheRTree.hits.isEmpty() ? null : cacheRTree.hits.size();
                System.out.println("hits for unlimited R-Tree cache total = " + hits);
                cacheRTreeHitsSummary.add(hits);

                Long hitsCoverageAverage = cacheUnlimited.hits.isEmpty() ? null : ((long) cacheUnlimited.hits.stream().mapToInt(v -> v).average().getAsDouble());
                System.out.println("hits unlimited cache coverage average = " +  hitsCoverageAverage);
                hitsCoverageAverageSummary.add(hitsCoverageAverage);

                hitsCoverageAverage = cacheRemoveMin.hits.isEmpty() ? null : ((long) cacheRemoveMin.hits.stream().mapToInt(v -> v).average().getAsDouble());
                System.out.println("hits limited cache coverage average = " +  hitsCoverageAverage);
                hitsRemoveMinCoverageAverageSummary.add(hitsCoverageAverage);

                hitsCoverageAverage = cacheRTree.hits.isEmpty() ? null : ((long) cacheRTree.hits.stream().mapToInt(v -> v).average().getAsDouble());
                System.out.println("hits unlimited R-Tree cache coverage average = " +  hitsCoverageAverage);
                hitsRTreeAverageSummary.add(hitsCoverageAverage);

                System.gc();
            }
        }

        for (int i=checkpointNum; i<=numOfQueries ; i+=checkpointNum){
            int curIndex = i/checkpointNum - 1;
            System.out.println("---------------------------------");
            System.out.println(i);
            System.out.println("no cache average : " + listNoCacheTimesSummary.get(curIndex));
            System.out.println("with cache average : " + listWithCacheTimesSummary.get(curIndex));
            System.out.println("with cache remove min average : " + listWithCacheRemoveMinTimesSummary.get(curIndex));
            System.out.println("with cache R-Tree average : " + listWithRTreeCacheTimesSummary.get(curIndex));
            System.out.println("hits unlimited cache total : " + cacheHitsSummary.get(curIndex));
            System.out.println("hits limited cache total : " + cacheRemoveMinHitsSummary.get(curIndex));
            System.out.println("hits unlimited R-Tree cache total : " + cacheRTreeHitsSummary.get(curIndex));
            System.out.println("hits unlimited cache coverage average = " +  hitsCoverageAverageSummary.get(curIndex));
            System.out.println("hits limited cache coverage average = " +  hitsRemoveMinCoverageAverageSummary.get(curIndex));
            System.out.println("hits unlimited R-Tree cache coverage average = " +  hitsRTreeAverageSummary.get(curIndex));
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
            if (cacheType == R_TREE || cacheType == KD_TREE){
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
                }
                while (it.hasNext()){
                    Set<Integer> curSet = cacheType == R_TREE ? (Set<Integer>)((RTreeIterator)it).next().value() : (Set<Integer>)((KDIterator)it).next().value();
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
                    } else {
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
}
