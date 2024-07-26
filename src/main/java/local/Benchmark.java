package local;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Benchmark {


    public static void main(String[] args) {
        runBenchmark(10, 1_000_000, 100_000, 1000, 3);
    }

    static void runBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int numOfIterations){

        //TODO
        // - organize the flow to be simple and readable
        // - add relevant classes - for table, interval, record (don't use apache pair)
        // - rename benchmark package to "cloud"
        // - try different params
        // - add cache data structure type (basic/enhanced/spatial type) and cache replacement type (unlimited, policy type)
        // - add missing tests / refactor existing
        // - add ReadMe, squash commits
        // - consider hashmap for cache instead of list (to avoid duplicate intervals)

        ArrayList<List<Long>> listNoCacheTimes = new ArrayList<>(numOfIterations);
        ArrayList<List<Long>> listWithCacheTimes = new ArrayList<>(numOfIterations);
        ArrayList<List<Integer>> cacheHits = new ArrayList<>(numOfIterations);

        for (int j=0; j<numOfIterations; j++) {

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

                System.gc();
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
        System.out.println("total cache hits num : " + cacheHits.stream().flatMap(l -> l.stream()).count());
        System.out.println("total cache hits coverage average : " + cacheHits.stream().flatMap(l -> l.stream()).mapToInt(v -> v).average().getAsDouble());
    }

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

    static int runQueryWithCache(HashMap<Integer, List<ArrayList<Integer>>> table, ArrayList<Pair<Integer, Integer>> interval, Cache cache){
        Set<Integer> coverage = new HashSet<>();
        Set<Integer> minCoverage = cache.getMinCoverage(interval);

        int resultCount = 0;
        if (minCoverage == null) {
            for (Map.Entry<Integer, List<ArrayList<Integer>>> entry : table.entrySet()) {
                for (ArrayList<Integer> record : entry.getValue()) {
                    if (Benchmark.intervalContainsRecord(interval, record)) {
                        resultCount++;
                        coverage.add(entry.getKey());
                        System.out.println("with cache-no-hit, found record number : " + resultCount);
                    }
                }
            }
        }else{
            for (int fileNum : minCoverage){
                for (ArrayList<Integer> record : table.get(fileNum)) {
                    if (Benchmark.intervalContainsRecord(interval, record)) {
                        resultCount++;
                        coverage.add(fileNum);
                        System.out.println("with cache-hit, found record number : " + resultCount);
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

        for (Map.Entry<Integer, List<ArrayList<Integer>>> entry : table.entrySet()) {
            for (ArrayList<Integer> record : entry.getValue()) {
                if (Benchmark.intervalContainsRecord(interval, record)) {
                    resultCount++;
                    System.out.println("no cache, found record number : " + resultCount);
                }
            }
        }

        return resultCount;
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
