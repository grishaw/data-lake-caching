package local;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Benchmark {

    static final long SLEEP_TO_SIMULATE_CLOUD_READ_NS = 10_000;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        runBenchmark(10, 1_000_000, 100_000, 1000, 2);
        long end = System.currentTimeMillis();

        System.out.println("benchmark took : " + (end-start) / 1000 / 60 + " minutes");
    }

    static void runBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int numOfIterations){

        //TODO
        // - organize the flow to be simple and readable
        // - add relevant classes - for table, interval, record (don't use apache pair)
        // - try different params
        // - add cache data structure type (basic/enhanced/spatial type) and cache replacement type (unlimited, policy type)
        // - add missing tests / refactor existing
        // - add ReadMe, squash commits
        // - consider hashmap for cache instead of list (to avoid duplicate intervals)
        // - consider running on AWS

        ArrayList<List<Long>> listNoCacheTimes = new ArrayList<>(numOfIterations);
        ArrayList<List<Long>> listWithCacheTimes = new ArrayList<>(numOfIterations);
        ArrayList<List<Integer>> cacheHits = new ArrayList<>(numOfIterations);

        for (int j=0; j<numOfIterations; j++) {

            System.gc();

            listNoCacheTimes.add(new LinkedList<>());
            listWithCacheTimes.add(new LinkedList<>());

            HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(numOfColumns, numOfRecords, numOfFiles);
            Cache cache = new Cache((int) Math.round(numOfFiles * 0.7));

            int resultNoCache;
            int resultWithCache;
            for (int i = 0; i < numOfQueries; i++) {

                ArrayList<Pair<Integer, Integer>> interval = generateInterval(numOfColumns, ThreadLocalRandom.current().nextDouble());

                long startNoCache = System.currentTimeMillis();
                resultNoCache = runQueryWithNoCache(table, interval);
                long endNoCache = System.currentTimeMillis();

                long startWithCache = System.currentTimeMillis();
                resultWithCache = runQueryWithCache(table, interval, cache);
                long endWithCache = System.currentTimeMillis();

                if (resultNoCache != resultWithCache){
                    throw new IllegalStateException("results do not match : " + resultNoCache + ", " + resultWithCache);
                }

                listNoCacheTimes.get(j).add(endNoCache - startNoCache);
                listWithCacheTimes.get(j).add(endWithCache - startWithCache);

                System.out.println(j + ", " + i + " done");
            }

            cacheHits.add(cache.hits);
        }

        // ------------------------------------------------

        System.out.println("--------------------------");

        for (int i=0; i<numOfIterations; i++) {
            System.out.println(i + ", no cache : " + listNoCacheTimes.get(i).stream().mapToLong(v -> v).average().getAsDouble());
            System.out.println(i + ", with cache : " + listWithCacheTimes.get(i).stream().mapToLong(v -> v).average().getAsDouble());
            System.out.println(i + ", hits = " + cacheHits.get(i));
            System.out.println(i + ", hits num = " + cacheHits.get(i).size());
            System.out.println(i + ", hits coverage average = " + cacheHits.get(i).stream().mapToInt(v -> v).average());
        }

        System.out.println("--------------------------");
        System.out.println("total no cache : " + listNoCacheTimes.stream().flatMap(l -> l.stream()).mapToLong(v -> v).average().getAsDouble());
        System.out.println("total with cache : " + listWithCacheTimes.stream().flatMap(l -> l.stream()).mapToLong(v -> v).average().getAsDouble());
        System.out.println("total cache hits num : " + cacheHits.stream().flatMap(l -> l.stream()).count() / numOfIterations);
        System.out.println("total cache hits coverage average : " + cacheHits.stream().flatMap(l -> l.stream()).mapToInt(v -> v)
                .average().toString());
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
            result.add(ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE));
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
                busyWait();
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
                busyWait();
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
            busyWait();
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
                int random1 = ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
                int random2 = ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);

                result.add(Pair.of(Math.min(random1, random2), Math.max(random1, random2)));
            }else{
                // no term - put min/max
                result.add(Pair.of(Integer.MIN_VALUE, Integer.MAX_VALUE));
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

    // to simulate that reading file from cloud takes some time
    static void busyWait(){
        long start = System.nanoTime();
        while(start +  SLEEP_TO_SIMULATE_CLOUD_READ_NS >= System.nanoTime());
    }

    // cache
    static class Cache {
        List<Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>>> cache;
        List<Integer> hits;

        int maxCoverage;

        public Cache(int maxCoverage){
            this.maxCoverage = maxCoverage;
            cache = new LinkedList<>();
            hits = new LinkedList<>();
        }

        Set<Integer> getMinCoverage (ArrayList<Pair<Integer, Integer>> queryInterval){
            Set<Integer> result = null;
            for (Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>> entry : cache){
                if (intervalContainsInterval(entry.getLeft(), queryInterval) && (result == null || result.size() > entry.getRight().size())){
                    result = entry.getRight();
                }
            }

            if (result != null){
                hits.add(result.size());
            }

            return result;
        }

        void put(ArrayList<Pair<Integer, Integer>> interval, Set<Integer> coverage){
            if (coverage.size() < maxCoverage){
                cache.add(Pair.of(interval, coverage));
            }
        }

    }
}
