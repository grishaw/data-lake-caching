package cloud;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/** Sums Spark task metrics per job group, so each query phase has its own read counters. */
final class PhaseMetrics extends SparkListener {

    static final class Totals {
        long bytesRead, recordsRead, tasks, executorRunMs, executorCpuNs, gcMs;
    }

    private final Map<Integer, String> stageGroup = new ConcurrentHashMap<>();
    private final Map<String, Totals> byGroup = new HashMap<>();

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted event) {
        Properties p = event.properties();
        String group = p == null ? null : p.getProperty("spark.jobGroup.id");
        if (group != null) stageGroup.put(event.stageInfo().stageId(), group);
    }

    @Override
    public synchronized void onTaskEnd(SparkListenerTaskEnd event) {
        String group = stageGroup.get(event.stageId());
        if (group == null || event.taskMetrics() == null) return;
        Totals t = byGroup.computeIfAbsent(group, k -> new Totals());
        t.bytesRead += event.taskMetrics().inputMetrics().bytesRead();
        t.recordsRead += event.taskMetrics().inputMetrics().recordsRead();
        t.executorRunMs += event.taskMetrics().executorRunTime();
        t.executorCpuNs += event.taskMetrics().executorCpuTime();
        t.gcMs += event.taskMetrics().jvmGCTime();
        t.tasks += 1;
    }

    synchronized Totals take(String group) {
        Totals t = byGroup.remove(group);
        return t == null ? new Totals() : t;
    }
}
