package com.fabric.foxtrot.ingestion.filter;


import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fabric.foxtrot.ingestion.FoxtrotProcessor;

import java.util.function.Predicate;

import static com.fabric.foxtrot.ingestion.FoxtrotProcessor.METRICS_REGISTRY;

/**
 * @author tushar.naik
 * @version 1.0  08/03/17 - 5:55 PM
 */
public class ValidNodeFilter implements Predicate<JsonNode> {

    private static final Meter invalidMeter =
            METRICS_REGISTRY.meter(MetricRegistry.name(FoxtrotProcessor.class, "invalid-event"));

    @Override
    public boolean test(JsonNode node) {
        if (node == null
                || isInvalidDocument(node)
                || isInvalidTimestamp(node)) {
            invalidMeter.mark();
            return false;
        }
        return true;
    }

    private boolean isInvalidDocument(JsonNode node) {
        return !node.has("id") || !node.has("time") || !node.has("app");
    }

    private boolean isInvalidTimestamp(JsonNode node) {
        return node.get("time").asLong() < 0;
    }
}
