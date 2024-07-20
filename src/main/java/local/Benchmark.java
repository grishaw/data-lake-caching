package local;

import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Benchmark {

    public static void main(String[] args) {

    }

    static HashMap<Integer, List<List<Double>>> createRandomTable(int numOfColumns, int numOfRecords, int numOfFiles){

        HashMap<Integer, List<List<Double>>> result = new HashMap<>();

        for (int i=0; i<numOfRecords; i++){
            List<Double> curRecord = generateRecord(numOfColumns);
            int curPartition = ThreadLocalRandom.current().nextInt(numOfFiles);
            result.computeIfAbsent(curPartition, x -> new LinkedList<>()).add(curRecord);
        }

        return result;
    }

    static List<Double> generateRecord(int numOfColumns){
        List<Double> result = new ArrayList<>(numOfColumns);

        for (int i=0; i<numOfColumns; i++){
            result.add(ThreadLocalRandom.current().nextDouble(Double.MIN_VALUE, Double.MAX_VALUE));
        }

        return result;
    }

}
