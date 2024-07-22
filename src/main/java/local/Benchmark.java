package local;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Benchmark {


    static List<Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>>> cache;

    public static void main(String[] args) {
        runBenchmark(10, 1_000_000, 100_000, 1000, 3);
    }

    static void runBenchmark(int numOfColumns, int numOfRecords, int numOfFiles, int numOfQueries, int numOfIterations){

        //TODO
        // - verify we get the same result for cache and no-cache (use records number)
        // - organize the flow to be simple and readable
        // - add relevant classes - for table, interval, record, cache
        // - do not use static cache, init per test
        // - rename benchmark package to "cloud"
        // - try different params
        // - add cache data structure type (basic/enhanced/spatial type) and cache replacement type (unlimited, policy type)

        List<Pair<Long, Long>> resultsTimes = new LinkedList<>();
        List<Integer> resultsCoverageWithCache = new LinkedList<>();

        for (int j=0; j<numOfIterations; j++) {

            HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(numOfColumns, numOfRecords, numOfFiles);
            cache = new LinkedList<>();

            long start = System.currentTimeMillis();
            for (int i = 0; i < numOfQueries; i++) {
                runRandomQuery(table, false);
            }
            long end = System.currentTimeMillis();

            long start2 = System.currentTimeMillis();
            for (int i = 0; i < numOfQueries; i++) {
                resultsCoverageWithCache.add(runRandomQuery(table, true));
            }
            long end2 = System.currentTimeMillis();

            System.out.println("iteration " + j + " : without cache = " + (end - start) + ", with cache = " + (end2 - start2));

            resultsTimes.add(Pair.of((end - start),(end2 - start2)));
        }

        System.out.println(resultsTimes);
        System.out.println(resultsCoverageWithCache);

        System.out.println("--------------------------");
        System.out.println("without cache : " + resultsTimes.stream().mapToLong(p -> p.getLeft()).average());
        System.out.println("with cache : " + resultsTimes.stream().mapToLong(p -> p.getRight()).average());
        System.out.println("cache hits : " + (resultsCoverageWithCache.stream().filter(r -> r != null).count() / numOfIterations));

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

    static Integer runRandomQuery(HashMap<Integer, List<ArrayList<Integer>>> table, boolean useCache){
        int numOfColumns = table.values().stream().findFirst().get().get(0).size();
        int numOfFiles = table.keySet().size();
        double percentageOfNonEmptyTerms = ThreadLocalRandom.current().nextDouble();

        ArrayList<Pair<Integer, Integer>> interval = generateInterval(numOfColumns, percentageOfNonEmptyTerms);

        Set<Integer> coverage = new HashSet<>();
        Set<Integer> minCoverage = null;

        if (useCache){
            minCoverage = getMinCoverage(cache, interval);
        }

        if (minCoverage == null) {

            for (Map.Entry<Integer, List<ArrayList<Integer>>> entry : table.entrySet()) {
                for (ArrayList<Integer> record : entry.getValue()) {
                    if (Benchmark.intervalContainsRecord(interval, record)) {
                        if (useCache) {
                            coverage.add(entry.getKey());
                        }
                    }
                }
            }

        }else{
            for (int fileNum : minCoverage){
                for (ArrayList<Integer> record : table.get(fileNum)) {
                    if (Benchmark.intervalContainsRecord(interval, record)) {
                        coverage.add(fileNum);
                    }
                }
            }
        }

        if (useCache && coverage.size() < numOfFiles * 0.7){
            cache.add(Pair.of(interval, coverage));
        }

        return minCoverage == null ? null : minCoverage.size();

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

    // null means not found, empty set means empty set coverage
    static Set<Integer> getMinCoverage(List<Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>>> cache, ArrayList<Pair<Integer, Integer>> queryInterval){

        Set<Integer> result = null;
        for (Pair<ArrayList<Pair<Integer, Integer>>, Set<Integer>> entry : cache){
            if (intervalContainsInterval(entry.getLeft(), queryInterval) && (result == null || result.size() > entry.getRight().size())){
                result = entry.getRight();
            }
        }

        return result;

    }

}
