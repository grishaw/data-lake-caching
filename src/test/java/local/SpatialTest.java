package local;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.tinspin.index.Index;
import org.tinspin.index.kdtree.KDIterator;
import org.tinspin.index.kdtree.KDTree;
import org.tinspin.index.phtree.PHTreeP;
import org.tinspin.index.qthypercube.QuadTreeKD;
import org.tinspin.index.rtree.RTree;
import org.tinspin.index.rtree.RTreeIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static local.Benchmark.getQueryMin;
import static local.Benchmark.mapIntervalToPoint;

public class SpatialTest {

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
