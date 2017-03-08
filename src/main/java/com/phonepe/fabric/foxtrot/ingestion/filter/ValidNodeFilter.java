package com.phonepe.fabric.foxtrot.ingestion.filter;


import com.fasterxml.jackson.databind.JsonNode;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * @author tushar.naik
 * @version 1.0  08/03/17 - 5:55 PM
 */
public class ValidNodeFilter implements Predicate<JsonNode> {

    private static final Meter invalidMeter = Metrics.newMeter(ValidNodeFilter.class, "invalid-document",
            "invalid-document", TimeUnit.SECONDS);

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
