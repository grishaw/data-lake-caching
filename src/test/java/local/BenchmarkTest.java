package local;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.tinspin.index.Index;
import org.tinspin.index.kdtree.KDIterator;
import org.tinspin.index.kdtree.KDTree;
import org.tinspin.index.phtree.PHTreeP;
import org.tinspin.index.qthypercube.QuadTreeKD;
import org.tinspin.index.rtree.RTree;
import org.tinspin.index.rtree.RTreeIterator;

import java.util.*;

import static local.Benchmark.*;

public class BenchmarkTest {

    @Test
    public void runBenchmarkTest(){
        runBenchmark(10, 100_000, 20_000, 100, 50,
                UNLIMITED_CACHE_CAPACITY, 0, 1, CacheType.LINKED_LIST, TestMode.SIMPLE);
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

    @Test
    public void oneDTest(){

        // intervals - [1,4], [3,6], [2,8], [6,9], [1,10]
        // query - [5,7]

        // points
        double [] p1 = new double[] {1.0,-4.0};
        double [] p2 = new double[] {3.0,-6.0};
        double [] p3 = new double[] {2.0,-8.0};
        double [] p4 = new double[] {6.0,-9.0};
        double [] p5 = new double[] {1.0,-10.0};

        //query - [-inf, 5] [-inf, -7]
        double [] qmin = new double[] {-1000000, -100000000};
        double [] qmax = new double[] {5, -7};

        System.out.println("------- R-TREE ----------------");

        RTree<String> rTree = RTree.createRStar(2);
        rTree.insert(p1, "p1");
        rTree.insert(p2, "p2");
        rTree.insert(p3, "p3");
        rTree.insert(p4, "p4");
        rTree.insert(p5, "p5");

        RTreeIterator<String> it = rTree.queryIntersect(qmin, qmax);
        while (it.hasNext()){
            System.out.println(it.next().value());
        }

        System.out.println("------- KD-TREE ----------------");

        KDTree<String> kdTree = KDTree.create(2);
        kdTree.insert(p1, "p1");
        kdTree.insert(p2, "p2");
        kdTree.insert(p3, "p3");
        kdTree.insert(p4, "p4");
        kdTree.insert(p5, "p5");

        KDIterator<String> it2 = kdTree.query(qmin, qmax);
        while (it2.hasNext()){
            System.out.println(it2.next().value());
        }

        System.out.println("------- QUAD-TREE ----------------");

        QuadTreeKD<String> quadTree = QuadTreeKD.create(2);
        quadTree.insert(p1, "p1");
        quadTree.insert(p2, "p2");
        quadTree.insert(p3, "p3");
        quadTree.insert(p4, "p4");
        quadTree.insert(p5, "p5");

        Index.PointIterator<String> it3 = quadTree.query(qmin, qmax);
        while (it3.hasNext()){
            System.out.println(it3.next().value());
        }

        System.out.println("------- PH-TREE ----------------");

        PHTreeP<String> phTree = PHTreeP.create(2);
        phTree.insert(p1, "p1");
        phTree.insert(p2, "p2");
        phTree.insert(p3, "p3");
        phTree.insert(p4, "p4");
        phTree.insert(p5, "p5");

        Index.PointIterator<String> it4 = phTree.query(qmin, qmax);
        while (it4.hasNext()){
            System.out.println(it4.next().value());
        }
    }

    @Test
    public void twoDTest(){
        // intervals - ([-1,2],[2,3]), ([1,4],[1,6]), ([3,5],[5,8]), ([5,10],[1,2])
        // query - ([2,3],[3,4]) --> {2, 3, -3, -4}
        // query - ([3,4],[5,6]) --> {3, 5, -4, -6}

        // points
        double [] p1 = new double[] {-1,2,-2,-3};
        double [] p2 = new double[] {1,1,-4,-6};
        double [] p3 = new double[] {3,5,-5,-8};
        double [] p4 = new double[] {5,1,-10,-2};

        //query
        double [] qmin = new double[] {-1_000_000, -1_000_000, -1_000_000, -1_000_000};
        double [] qmax = new double[] {2, 3, -3, -4};

        System.out.println("------- R-TREE ----------------");

        RTree <String> rTree = RTree.createRStar(4);
        rTree.insert(p1, "p1");
        rTree.insert(p2, "p2");
        rTree.insert(p3, "p3");
        rTree.insert(p4, "p4");

        RTreeIterator<String> it = rTree.queryIntersect(qmin, qmax);
        while (it.hasNext()){
            System.out.println(it.next().value());
        }

        System.out.println("------- KD-TREE ----------------");

        KDTree<String> kdTree = KDTree.create(4);
        kdTree.insert(p1, "p1");
        kdTree.insert(p2, "p2");
        kdTree.insert(p3, "p3");
        kdTree.insert(p4, "p4");

        KDIterator<String> it2 = kdTree.query(qmin, qmax);
        while (it2.hasNext()){
            System.out.println(it2.next().value());
        }

        System.out.println("------- QUAD-TREE ----------------");

        QuadTreeKD <String> quadTree = QuadTreeKD.create(4);
        quadTree.insert(p1, "p1");
        quadTree.insert(p2, "p2");
        quadTree.insert(p3, "p3");
        quadTree.insert(p4, "p4");

        Index.PointIterator<String> it3 = quadTree.query(qmin, qmax);
        while (it3.hasNext()){
            System.out.println(it3.next().value());
        }

        System.out.println("------- PH-TREE ----------------");

        PHTreeP<String> phTree = PHTreeP.create(4);
        phTree.insert(p1, "p1");
        phTree.insert(p2, "p2");
        phTree.insert(p3, "p3");
        phTree.insert(p4, "p4");

        Index.PointIterator<String> it4 = phTree.query(qmin, qmax);
        while (it4.hasNext()){
            System.out.println(it4.next().value());
        }
    }

    @Test
    public void mapIntervalToPointTest(){
        ArrayList<Pair<Integer, Integer>> interval = new ArrayList<>(Arrays.asList(Pair.of(1,10), Pair.of(2, 9)));
        System.out.println(Arrays.toString(mapIntervalToPoint(interval)));
    }

    @Test
    public void testCoverageSet(){
        // points
        double [] p1 = new double[] {-1,2,-2,-3};
        double [] p2 = new double[] {1,1,-4,-6};
        double [] p3 = new double[] {3,5,-5,-8};
        double [] p4 = new double[] {5,1,-10,-2};

        //query
        double [] qmin = new double[] {-1_000_000, -1_000_000, -1_000_000, -1_000_000};
        double [] qmax = new double[] {2, 3, -3, -4};

        System.out.println("------- R-TREE ----------------");

        RTree <Set<Integer>> rTree = RTree.createRStar(4);
        rTree.insert(p1, new HashSet<>(Arrays.asList(1,2)));
        rTree.insert(p2, new HashSet<>(Arrays.asList(2,3)));
        rTree.insert(p3, new HashSet<>(Arrays.asList(3,4)));
        rTree.insert(p4, new HashSet<>(Arrays.asList(4,5)));

        RTreeIterator<Set<Integer>> it = rTree.queryIntersect(qmin, qmax);
        while (it.hasNext()){
            System.out.println(it.next().value());
        }
    }

    @Test
    public void myTest(){
        int numOfColumns = 2;

        Index rTree = RTree.createRStar(numOfColumns * 2);

        ArrayList<Pair<Integer, Integer>> interval = new ArrayList<>(Arrays.asList(Pair.of(-3545,3000), Pair.of(-1000, 2000)));

        ArrayList<Pair<Integer, Integer>> interval2 = new ArrayList<>(Arrays.asList(Pair.of(-3045,3000), Pair.of(-1000, 2000)));

        Set<Integer> coverage = new HashSet<>(Arrays.asList(1,2,3));

        ((RTree) rTree).insert(mapIntervalToPoint(interval), coverage);

        ((RTree) rTree).insert(mapIntervalToPoint(interval2), new HashSet<>(Arrays.asList(1,2)));

        ////////////////////

        ArrayList<Pair<Integer, Integer>> queryInterval = new ArrayList<>(Arrays.asList(Pair.of(-2000,2000), Pair.of(-100, 200)));

        double [] queryMin = getQueryMin (queryInterval.size() * 2);
        double [] queryMax = mapIntervalToPoint(queryInterval);

        if (queryMin.length != queryMax.length){
            throw new IllegalStateException("lengths do not match");
        }

        Set<Integer> result = null;
        RTreeIterator<Set<Integer>> it = ((RTree)rTree).queryIntersect(queryMin, queryMax);
        while (it.hasNext()){
            Set<Integer> curSet = it.next().value();
            if (result == null || result.size() > curSet.size()){
                result = curSet;
            }
        }

        System.out.println(result);
    }
}
