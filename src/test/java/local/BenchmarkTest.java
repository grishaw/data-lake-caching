package local;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static local.Benchmark.*;

public class BenchmarkTest {

    @Test
    public void runBenchmarkTest(){
        runBenchmark(5, 100_000, 20_000, 2000, 100, 100, 0, 1, CacheType.LINKED_LIST);
    }

    @Test
    public void createRandomTableTest(){

        final int NUM_OF_FILES = 100;
        final int NUM_OF_COLUMNS = 10;
        final int NUM_OF_RECORDS = 100_000;

        HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(NUM_OF_COLUMNS, NUM_OF_RECORDS, NUM_OF_FILES);

        Assertions.assertEquals(NUM_OF_FILES, table.size());

        int someKey = table.keySet().stream().findFirst().get();
        List<ArrayList<Integer>> somePartition = table.get(someKey);

        Assertions.assertTrue(!somePartition.isEmpty() && somePartition.size() < NUM_OF_RECORDS);
        Assertions.assertEquals(NUM_OF_COLUMNS, somePartition.get(0).size());

        int total = 0;
        for (Integer key : table.keySet()){
            total += table.get(key).size();
        }
        Assertions.assertEquals(NUM_OF_RECORDS, total);

    }

    @Test
    public void generateRecordTest(){
        List<Integer> record = generateRecord(5);
        System.out.println(record);
    }

    @Test
    public void runQueryWithCacheTest(){
        final int NUM_OF_FILES = 10000;
        final int NUM_OF_COLUMNS = 10;
        final int NUM_OF_RECORDS = 1_000_000;

        HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(NUM_OF_COLUMNS, NUM_OF_RECORDS, NUM_OF_FILES);

        ArrayList<Pair<Integer, Integer>> interval = generateInterval(NUM_OF_COLUMNS, 0.5);

        Integer result = runQueryWithCache(table, interval, new Cache(10000, 100, NUM_OF_COLUMNS, 0, 1, CacheType.LINKED_LIST));

        System.out.println(result);
    }

    @Test
    public void runQueryWithNoCacheTest(){

        final int NUM_OF_FILES = 10000;
        final int NUM_OF_COLUMNS = 10;
        final int NUM_OF_RECORDS = 1_000_000;

        HashMap<Integer, List<ArrayList<Integer>>> table = createRandomTable(NUM_OF_COLUMNS, NUM_OF_RECORDS, NUM_OF_FILES);

        ArrayList<Pair<Integer, Integer>> interval = generateInterval(NUM_OF_COLUMNS, 0.5);

        Integer result = runQueryWithNoCache(table, interval);

        System.out.println(result);

    }

    @Test
    public void generateIntervalTest(){
        List<Pair<Integer, Integer>> interval = generateInterval(6, 0.5);
        System.out.println(interval);
    }
    
    @Test
    public void intervalContainsRecordTest(){
        ArrayList<Integer> record = new ArrayList<>(Arrays.asList(100, 20, 45, -4, 37));
        Pair<Integer, Integer> all = Pair.of(Integer.MIN_VALUE, Integer.MAX_VALUE);
        ArrayList<Pair<Integer, Integer>> interval = new ArrayList<>(Arrays.asList(all, all, all, all, all));

        Assertions.assertTrue(intervalContainsRecord(interval, record));

        interval.set(0, Pair.of(99,101));
        Assertions.assertTrue(intervalContainsRecord(interval, record));

        interval.set(1, Pair.of(-10,10));
        Assertions.assertFalse(intervalContainsRecord(interval, record));

    }

    @Test
    public void intervalContainsIntervalTest(){
        Pair<Integer, Integer> all = Pair.of(Integer.MIN_VALUE, Integer.MAX_VALUE);
        ArrayList<Pair<Integer, Integer>> interval1 = new ArrayList<>(Arrays.asList(all, all, all, all, all));
        ArrayList<Pair<Integer, Integer>> interval2 = new ArrayList<>(Arrays.asList(all, all, all, all, all));

        Assertions.assertTrue(intervalContainsInterval(interval1, interval1));
        Assertions.assertTrue(intervalContainsInterval(interval1, interval2));

        interval1.set(0, Pair.of(0,100));
        Assertions.assertFalse(intervalContainsInterval(interval1, interval2));

        interval2.set(0, Pair.of(-10,100));
        Assertions.assertFalse(intervalContainsInterval(interval1, interval2));

        interval2.set(0, Pair.of(5,100));
        Assertions.assertTrue(intervalContainsInterval(interval1, interval2));

        interval1.set(3, Pair.of(-10,10));
        Assertions.assertFalse(intervalContainsInterval(interval1, interval2));

        interval2.set(3, Pair.of(-10,10));
        Assertions.assertTrue(intervalContainsInterval(interval1, interval2));

    }

    @Test
    public void getMinCoverageTest(){

        Cache cache = new Cache(100,-1, 3, 0, 0, CacheType.LINKED_LIST);

        ArrayList<Pair<Integer, Integer>> interval1
                = new ArrayList<>(Arrays.asList(Pair.of(0,10), Pair.of(20,1000), Pair.of(-10, -5)));

        Set<Integer> coverage1 = new HashSet<>(Arrays.asList(10,20,30));

        ArrayList<Pair<Integer, Integer>> interval2
                = new ArrayList<>(Arrays.asList(Pair.of(Integer.MIN_VALUE,Integer.MAX_VALUE), Pair.of(20,1000), Pair.of(-10, -5)));

        ArrayList<Pair<Integer, Integer>> interval3
                = new ArrayList<>(Arrays.asList(Pair.of(0,0), Pair.of(0,10), Pair.of(-10, 0)));


        Set<Integer> coverage2 = new HashSet<>(Arrays.asList(10,20,30,40));

        cache.put(interval1, coverage1);
        cache.put(interval2, coverage2);
        cache.put(interval3, new HashSet<>());


        ArrayList<Pair<Integer, Integer>> myInterval1
                = new ArrayList<>(Arrays.asList(Pair.of(Integer.MIN_VALUE,Integer.MAX_VALUE), Pair.of(30,50), Pair.of(-2, -1)));

        ArrayList<Pair<Integer, Integer>> myInterval2
                = new ArrayList<>(Arrays.asList(Pair.of(0,0), Pair.of(30,50), Pair.of(-8, -6)));

        ArrayList<Pair<Integer, Integer>> myInterval3
                = new ArrayList<>(Arrays.asList(Pair.of(-100,0), Pair.of(30,50), Pair.of(-8, -6)));

        ArrayList<Pair<Integer, Integer>> myInterval4
                = new ArrayList<>(Arrays.asList(Pair.of(0,0), Pair.of(2,3), Pair.of(-8, -6)));

        Assertions.assertNull(cache.getMinCoverage(myInterval1));

        Assertions.assertEquals(3, cache.getMinCoverage(myInterval2).size());

        Assertions.assertEquals(4, cache.getMinCoverage(myInterval3).size());

        Assertions.assertEquals(0, cache.getMinCoverage(myInterval4).size());

    }
}
