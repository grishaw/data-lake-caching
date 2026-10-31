package cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/** One predicate from a saved trace. All bounds are closed, as in the original query condition. */
final class TraceQuery {
    final int seq;
    final String klass;
    final String shipFrom;
    final String shipTo;
    final double discountFrom;
    final double discountTo;
    final int quantityFrom;
    final int quantityTo;
    final String template;

    TraceQuery(int seq, String klass, String shipFrom, String shipTo,
               double discountFrom, double discountTo, int quantityFrom, int quantityTo) {
        this(seq, klass, shipFrom, shipTo, discountFrom, discountTo, quantityFrom, quantityTo, "q6");
    }

    TraceQuery(int seq, String klass, String shipFrom, String shipTo,
               double discountFrom, double discountTo, int quantityFrom, int quantityTo, String template) {
        this.seq = seq;
        this.klass = klass;
        this.shipFrom = shipFrom;
        this.shipTo = shipTo;
        this.discountFrom = discountFrom;
        this.discountTo = discountTo;
        this.quantityFrom = quantityFrom;
        this.quantityTo = quantityTo;
        this.template = template;
    }

    boolean isQ1() {
        return "q1".equals(template);
    }

    Benchmark.ConditionValues toCondition(List<String> files) {
        return new Benchmark.ConditionValues(shipFrom, shipTo, discountFrom, discountTo, quantityFrom, quantityTo, files);
    }

    static List<TraceQuery> parse(List<String> jsonLines) throws java.io.IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<TraceQuery> queries = new ArrayList<>();
        for (String line : jsonLines) {
            if (line.trim().isEmpty()) continue;
            JsonNode n = mapper.readTree(line);
            queries.add(new TraceQuery(n.get("seq").asInt(), n.get("klass").asText(),
                    n.get("ship_from").asText(), n.get("ship_to").asText(),
                    n.get("disc_from").asDouble(), n.get("disc_to").asDouble(),
                    n.get("qty_from").asInt(), n.get("qty_to").asInt(), n.path("template").asText("q6")));
        }
        return queries;
    }
}
