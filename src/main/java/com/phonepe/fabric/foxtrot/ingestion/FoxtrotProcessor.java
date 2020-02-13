package com.phonepe.fabric.foxtrot.ingestion;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.ClientType;
import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.FoxtrotClient;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.google.common.collect.Lists;
import com.olacabs.fabric.compute.ProcessingContext;
import com.olacabs.fabric.compute.processor.InitializationException;
import com.olacabs.fabric.compute.processor.ProcessingException;
import com.olacabs.fabric.compute.processor.StreamingProcessor;
import com.olacabs.fabric.compute.util.ComponentPropertyReader;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.olacabs.fabric.model.event.EventSet;
import com.olacabs.fabric.model.processor.Processor;
import com.olacabs.fabric.model.processor.ProcessorType;
import com.phonepe.fabric.foxtrot.ingestion.filter.ValidNodeFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * A Processor implementation that publishes events to foxtrot
 */
@Processor(namespace = "global",
        name = "foxtrot-processor",
        version = "1.3",
        cpu = 0.1,
        memory = 32,
        description = "A processor that publishes events to Foxtrot",
        processorType = ProcessorType.EVENT_DRIVEN,
        requiredProperties = {"foxtrot.host", "foxtrot.port"},
        optionalProperties = {"errorTable", "maxAppNameSize"})
@Slf4j
public class FoxtrotProcessor extends StreamingProcessor {

    public static final MetricRegistry METRICS_REGISTRY = SharedMetricRegistries.getOrCreate("metrics-registry");
    private static final Meter totalEventRateMeter =
            METRICS_REGISTRY.meter(MetricRegistry.name(FoxtrotProcessor.class, "total-event-set-rate"));
    private static final Meter validEventRateMeter =
            METRICS_REGISTRY.meter(MetricRegistry.name(FoxtrotProcessor.class, "valid-event-set-rate"));
    private static final List<String> MESSAGES_TO_IGNORE = Lists.newArrayList("Request-URI Too Long");

    private static final String ERROR = "ingestionException";
    private static final String ERROR_MESSAGE = "ingestionExceptionMessage";
    private static final String APP_NAME = "app";
    private String errorTableName;
    private int maxAppNameSize;

    private FoxtrotClient foxtrotClient;
    private ObjectMapper mapper;

    @Override
    public void initialize(String s, Properties global, Properties local,
            ComponentMetadata componentMetadata) throws InitializationException {

        /* foxtrot client setup */
        String foxtrotHost = ComponentPropertyReader.readString(local, global,
                "foxtrot.host", s, componentMetadata, "localhost");
        Integer foxtrotPort = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.port", s, componentMetadata, 80);
        errorTableName = ComponentPropertyReader.readString(local, global,
                "errorTable", s, componentMetadata, "debug");
        maxAppNameSize = ComponentPropertyReader
                .readInteger(local, global, "maxAppNameSize", s, componentMetadata, 1024);

        FoxtrotClientConfig foxtrotClientConfig = new FoxtrotClientConfig();
        foxtrotClientConfig.setClientType(ClientType.sync);
        foxtrotClientConfig.setHost(foxtrotHost);
        foxtrotClientConfig.setPort(foxtrotPort);
        foxtrotClientConfig.setTable("dummy");

        try {
            log.info("Creating foxtrot client- {}:{} table- {}", foxtrotClientConfig.getHost(),
                    foxtrotClientConfig.getPort(), foxtrotClientConfig.getTable());
            foxtrotClient = new FoxtrotClient(foxtrotClientConfig);
        } catch (Exception e) {
            log.error(String.format("Error creating foxtrot client with hosts%s, port:%s", foxtrotHost, foxtrotPort),
                    e);
            throw new RuntimeException(e);
        }
        mapper = new ObjectMapper();
    }

    @Override
    protected EventSet consume(ProcessingContext processingContext, EventSet eventSet) throws ProcessingException {

        totalEventRateMeter.mark(eventSet.getEvents().size());
        /*
         -> map eventSet (bytes) to Tree Node
         -> filter invalid data
         -> map them to AppDocument
         -> group by app to create a map of App -> List of Documents
        */
        Map<String, List<Document>> payloads = eventSet.getEvents()
                .stream()
                .map(payload -> {
                    try {
                        return mapper.readTree((byte[]) payload.getData());
                    } catch (IOException e) {
                        log.error("Unable to read payload.data as a tree", e);
                        throw new RuntimeException(e);
                    }
                })
                .filter(new ValidNodeFilter())
                .map(node -> AppDocuments
                        .builder()
                        .app(node.get(APP_NAME).asText())
                        .document(new Document(node.get("id").asText(), node.get("time").asLong(), node))
                        .build())
                .collect(Collectors.groupingBy(AppDocuments::getApp,
                        Collectors.mapping(AppDocuments::getDocument, Collectors.toList())));

        log.info("Received {} payloads", eventSet.getEvents().size());
        payloads.entrySet()
                .forEach(k -> log.info(k.getKey() + ":" + k.getValue().size()));

        validEventRateMeter.mark(eventSet.getEvents().size());

        payloads.entrySet()
                .forEach(entry -> {
                    final String app = entry.getKey();
                    final List<Document> documents = entry.getValue();
                    String sample = documents.isEmpty()
                            ? "N/A" : documents.get(0).getData() == null
                            ? "N/A" : documents.get(0).getData().toString();
                    try {
                        /* logging a dummy sample data from the list of documents, for debugging purposes */
                        log.info("Sending to Foxtrot app:{} size:{} sample:{}",
                                app, documents.size(), sample);
                        foxtrotClient.send(app, documents);
                        log.info("Published to Foxtrot successfully.  app:{} size:{} sample:{}",
                                app, documents.size(), sample);
                    } catch (Exception e) {
                        log.error("Failed to send document list:" + app
                                + " size:" + documents.size() + " sample:" + sample, e);
                        ingestFailedDocuments(app, documents, sample, e);
                        for (String message : MESSAGES_TO_IGNORE) {
                            if (e.getMessage().contains(message)) {
                                return;
                            }
                        }
                        throw new RuntimeException(e);
                    }
                });
        return null;
    }

    private void ingestFailedDocuments(String app, List<Document> documents, String sample, Exception exception) {
        try {
            List<Document> failedDocuments = new ArrayList<>();
            for (Document document : documents) {
                Map<String, Object> data = readMapFromObject(document.getData());
                data.put(ERROR, exception.getClass().getName());
                data.put(ERROR_MESSAGE, exception.getMessage());
                data.put(APP_NAME, app.length() > maxAppNameSize ? app.substring(0, maxAppNameSize) : app);
                document.setData(mapper.valueToTree(data));
                failedDocuments.add(document);
            }
            foxtrotClient.send(errorTableName, failedDocuments);
            log.info("Successfully sent failed documents to debug table for exception :{}, {}",
                    exception.getClass().getName(), exception.getMessage());
        } catch (Exception ex) {
            log.error("Error sending failed document list:" + app
                    + " size:" + documents.size() + " sample:" + sample, ex);
        }
    }

    @Override
    public void destroy() {
        try {
            foxtrotClient.close();
        } catch (Exception e) {
            log.error("Error while closing foxtrot client", e);
        }
    }

    private Map<String, Object> readMapFromObject(Object obj) {
        return mapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
        });
    }

    @Data
    @Builder
    @EqualsAndHashCode
    static class AppDocuments {

        private String app;
        private Document document;
    }
}
