package cloud;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Bounded entry store around the original ConditionValues list.
 * Containment lookup is the original Benchmark.getFilesFromCache, unchanged.
 * Sizes are bytes of a defined serialized form, so a budget means the same thing for every method.
 */
final class CacheStore {
    /** Two ISO dates (10 bytes each), two doubles, two ints. */
    static final int PREDICATE_BYTES = 10 + 10 + 8 + 8 + 4 + 4;
    static final int PER_STRING_OVERHEAD = 4;

    final LinkedList<Benchmark.ConditionValues> entries = new LinkedList<>();
    private final long capacityBytes;
    private final double w1;
    private final double w2;
    private long usedBytes = 0;
    long evictions = 0;
    long rejectedTooLarge = 0;

    CacheStore(long capacityBytes, double w1, double w2) {
        this.capacityBytes = capacityBytes;
        this.w1 = w1;
        this.w2 = w2;
    }

    long usedBytes() {
        return usedBytes;
    }

    static long entryBytes(Benchmark.ConditionValues e) {
        long total = PREDICATE_BYTES;
        for (String f : e.files) {
            total += PER_STRING_OVERHEAD + (f == null ? 0 : f.getBytes(StandardCharsets.UTF_8).length);
        }
        return total;
    }

    /** Original containment rule: smallest coverage among entries that contain the query. */
    List<String> lookupContained(Benchmark.ConditionValues query) {
        return Benchmark.getFilesFromCache(entries, query);
    }

    /** Exact-predicate rule, using the original equals. */
    List<String> lookupExact(Benchmark.ConditionValues query) {
        for (Benchmark.ConditionValues e : entries) {
            if (e.equals(query)) return e.files;
        }
        return null;
    }

    boolean hasExact(Benchmark.ConditionValues query) {
        return lookupExact(query) != null;
    }

    /** Whether this exact entry is still stored: a new entry can be its own eviction victim, or be too large to admit. */
    boolean holds(Benchmark.ConditionValues entry) {
        for (Benchmark.ConditionValues e : entries) {
            if (e == entry) return true;
        }
        return false;
    }

    /** Adds the entry, then evicts by Eq. 5 until the budget holds. Returns entries evicted by this put. */
    int put(Benchmark.ConditionValues entry) {
        long size = entryBytes(entry);
        if (capacityBytes >= 0 && size > capacityBytes) {
            rejectedTooLarge++;
            return 0;
        }
        entries.add(entry);
        usedBytes += size;
        int evicted = 0;
        while (capacityBytes >= 0 && usedBytes > capacityBytes) {
            removeLowestScore();
            evicted++;
        }
        evictions += evicted;
        return evicted;
    }

    /** Eq. 1: product of interval widths in raw units. */
    static double volume(Benchmark.ConditionValues e) {
        double days = LocalDate.parse(e.shipDateTo).toEpochDay() - LocalDate.parse(e.shipDateFrom).toEpochDay();
        return days * (e.discountTo - e.discountFrom) * (e.quantityTo - e.quantityFrom);
    }

    /** Eq. 5, with a metric that has no spread contributing 0. */
    static double score(double w1, double w2, double volume, double vMin, double vMax,
                        double coverage, double covMin, double covMax) {
        double v = vMax > vMin ? (volume - vMin) / (vMax - vMin) : 0.0;
        double c = covMax > covMin ? 1.0 - (coverage - covMin) / (covMax - covMin) : 0.0;
        return w1 * v + w2 * c;
    }

    // shortcut: linear scan instead of the paper's heap, overstates eviction cost for large caches; use a heap if a cache exceeds a few thousand entries
    private void removeLowestScore() {
        double vMin = Double.POSITIVE_INFINITY, vMax = Double.NEGATIVE_INFINITY;
        double cMin = Double.POSITIVE_INFINITY, cMax = Double.NEGATIVE_INFINITY;
        for (Benchmark.ConditionValues e : entries) {
            double v = volume(e);
            double c = e.files.size();
            vMin = Math.min(vMin, v);
            vMax = Math.max(vMax, v);
            cMin = Math.min(cMin, c);
            cMax = Math.max(cMax, c);
        }
        Benchmark.ConditionValues victim = null;
        double lowest = Double.POSITIVE_INFINITY;
        for (Benchmark.ConditionValues e : entries) {
            double g = score(w1, w2, volume(e), vMin, vMax, e.files.size(), cMin, cMax);
            // ties go to the oldest entry
            if (g < lowest) {
                lowest = g;
                victim = e;
            }
        }
        for (Iterator<Benchmark.ConditionValues> it = entries.iterator(); it.hasNext(); ) {
            if (it.next() == victim) {
                it.remove();
                usedBytes -= entryBytes(victim);
                return;
            }
        }
        throw new IllegalStateException("eviction victim not found");
    }
}
