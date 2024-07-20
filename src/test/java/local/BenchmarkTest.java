package local;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static local.Benchmark.createRandomTable;

public class BenchmarkTest {

    @Test
    public void createRandomTableTest(){
        HashMap<Integer, List<List<Double>>> table = createRandomTable(10, 100_000, 100);

        Assertions.assertEquals(100, table.size());

        int someKey = table.keySet().stream().findFirst().get();
        List<List<Double>> somePartition = table.get(someKey);

        Assertions.assertTrue(!somePartition.isEmpty() && somePartition.size() < 100_000);
        Assertions.assertEquals(10, somePartition.get(0).size());

        int total = 0;
        for (Integer key : table.keySet()){
            total += table.get(key).size();
        }
        Assertions.assertEquals(100_000, total);

    }
}
