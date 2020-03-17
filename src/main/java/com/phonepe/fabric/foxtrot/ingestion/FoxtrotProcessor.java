package com.phonepe.fabric.foxtrot.ingestion;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.ClientType;
import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.FoxtrotClient;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.olacabs.fabric.compute.ProcessingContext;
import com.olacabs.fabric.compute.processor.InitializationException;
import com.olacabs.fabric.compute.processor.ProcessingException;
import com.olacabs.fabric.compute.processor.StreamingProcessor;
import com.olacabs.fabric.compute.util.ComponentPropertyReader;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.olacabs.fabric.model.event.Event;
import com.olacabs.fabric.model.event.EventSet;
import com.olacabs.fabric.model.processor.Processor;
import com.olacabs.fabric.model.processor.ProcessorType;
import com.phonepe.fabric.foxtrot.ingestion.errorhandler.ErrorHandler;
import com.phonepe.fabric.foxtrot.ingestion.errorhandler.ErrorHandler.ErrorHandlerType;
import com.phonepe.fabric.foxtrot.ingestion.errorhandler.ErrorHandlerFactory;
import com.phonepe.fabric.foxtrot.ingestion.filter.ValidNodeFilter;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.phonepe.fabric.foxtrot.ingestion.errorhandler.ErrorHandler.ErrorHandlerType.SIDELINE_TOPOLOGY_ERROR_HANDLER;
import static com.phonepe.fabric.foxtrot.ingestion.utils.Utils.*;

/**
 * A Processor implementation that publishes events to foxtrot
 */
@Processor(namespace = "global",
        name = "foxtrot-processor",
        version = "1.6-SNAPSHOT",
        cpu = 0.1,
        memory = 32,
        description = "A processor that publishes events to Foxtrot",
        processorType = ProcessorType.EVENT_DRIVEN,
        requiredProperties = {"foxtrot.host", "foxtrot.port", "errorHandler"},
        optionalProperties = {"errorTable", "ignorableFailureMessagePatterns", "foxtrot.client.batchSize",
                "foxtrot.client.maxConnections", "foxtrot.client.keepAliveTimeMillis", "foxtrot.client.connectTimeoutMs",
                "foxtrot.client.opTimeoutMs", "foxtrot.client.callTimeOutMs"})
@Slf4j
public class FoxtrotProcessor extends StreamingProcessor {

    public static final MetricRegistry METRICS_REGISTRY = SharedMetricRegistries.getOrCreate("metrics-registry");
    private static final Meter totalEventRateMeter =
            METRICS_REGISTRY.meter(MetricRegistry.name(FoxtrotProcessor.class, "total-event-set-rate"));
    private static final Meter validEventRateMeter =
            METRICS_REGISTRY.meter(MetricRegistry.name(FoxtrotProcessor.class, "valid-event-set-rate"));

    public static final String APP_NAME = "app";

    private FoxtrotClient foxtrotClient;
    private ObjectMapper mapper;
    private ValidNodeFilter validNodeFilter;
    private ErrorHandler errorHandler;

    @Override
    public void initialize(String s, Properties global, Properties local,
                           ComponentMetadata componentMetadata) throws InitializationException {

        /* foxtrot client setup */
        FoxtrotClientConfig foxtrotClientConfig = getFoxtrotClientConfig(s, global, local, componentMetadata);

        try {
            log.info("Creating foxtrot client with config - {}", foxtrotClientConfig);
            foxtrotClient = new FoxtrotClient(foxtrotClientConfig);
        } catch (Exception e) {
            log.error(String.format("Error creating foxtrot client with hosts%s, port:%s",
                    foxtrotClientConfig.getHost(), foxtrotClientConfig.getPort()), e);
            throw new RuntimeException(e);
        }

        mapper = new ObjectMapper();
        validNodeFilter = new ValidNodeFilter();

        String errorHandlerType = ComponentPropertyReader.readString(local, global,
                "errorHandler", s, componentMetadata, SIDELINE_TOPOLOGY_ERROR_HANDLER.name());

        ErrorHandlerFactory errorHandlerFactory = new ErrorHandlerFactory(s, global, local, componentMetadata, foxtrotClient, mapper);

        this.errorHandler = errorHandlerFactory.get(ErrorHandlerType.valueOf(errorHandlerType));
        log.info("Created foxtrot processor with error handler: {}", this.errorHandler.getHandlerType());

    }

    private FoxtrotClientConfig getFoxtrotClientConfig(String s, Properties global, Properties local, ComponentMetadata componentMetadata) {
        String foxtrotHost = ComponentPropertyReader.readString(local, global,
                "foxtrot.host", s, componentMetadata, "localhost");
        Integer foxtrotPort = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.port", s, componentMetadata, 80);
        String ignorableFailureMessagePatterns = ComponentPropertyReader.readString(local, global,
                "ignorableFailureMessagePatterns", s, componentMetadata, null);

        Integer batchSize = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.client.batchSize", s, componentMetadata, 200);

        Integer maxConnections = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.client.maxConnections", s, componentMetadata, 10);
        Integer keepAliveTimeMillis = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.client.keepAliveTimeMillis", s, componentMetadata, 30000);
        Integer connectTimeoutMs = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.client.connectTimeoutMs", s, componentMetadata, 10);
        Integer opTimeoutMs = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.client.opTimeoutMs", s, componentMetadata, 10000);
        Integer callTimeOutMs = ComponentPropertyReader.readInteger(local, global,
                "foxtrot.client.callTimeOutMs", s, componentMetadata, 5000);

        FoxtrotClientConfig foxtrotClientConfig = new FoxtrotClientConfig();
        foxtrotClientConfig.setClientType(ClientType.sync);
        foxtrotClientConfig.setHost(foxtrotHost);
        foxtrotClientConfig.setPort(foxtrotPort);
        foxtrotClientConfig.setTable("dummy");
        foxtrotClientConfig.setBatchSize(batchSize);
        foxtrotClientConfig.setKeepAliveTimeMillis(keepAliveTimeMillis);
        foxtrotClientConfig.setMaxConnections(maxConnections);
        foxtrotClientConfig.setConnectTimeoutMs(connectTimeoutMs);
        foxtrotClientConfig.setOpTimeoutMs(opTimeoutMs);
        foxtrotClientConfig.setCallTimeOutMs(callTimeOutMs);

        if (Strings.isNotBlank(ignorableFailureMessagePatterns)) {
            List<String> ignorableFailureMessagePatternList = Arrays.asList(ignorableFailureMessagePatterns.split(","));
            foxtrotClientConfig.setIgnorableFailureMessagePatterns(ignorableFailureMessagePatternList);
        }
        return foxtrotClientConfig;
    }

    @Override
    protected EventSet consume(ProcessingContext processingContext, EventSet eventSet) throws ProcessingException {

        totalEventRateMeter.mark(eventSet.getEvents().size());

        log.info("Received {} payloads", eventSet.getEvents().size());
        Map<String, List<AppDocument>> payloads = getValidAppDocuments(eventSet);

        payloads.forEach((key, value) -> log.info(key + ":" + value.size()));

        Map<String, EventDocuments> appEventDocumentsMap = getAppEventDocumentsMap(payloads);

        List<Event> failedEvents = new ArrayList<>();

        appEventDocumentsMap.forEach((app, eventDocuments) -> {
            processEventDocuments(failedEvents, app, eventDocuments);
        });

        log.debug("Returning event set with failed events : {}", failedEvents);
        return EventSet.eventFromEventBuilder()
                .partitionId(eventSet.getPartitionId())
                .events(failedEvents)
                .build();
    }

    private void processEventDocuments(List<Event> failedEvents, String app, EventDocuments eventDocuments) {
        List<Document> documents = eventDocuments.getDocuments();
        List<Event> events = eventDocuments.getEvents();

        String sample = getSampleDocument(documents);
        try {
            publishFoxtrotEvent(app, documents, sample);
        } catch (Exception e) {
            String appName = app;
            try {
                // Retry event publish if it's erroneous app name
                if (isErroneousAppName(app)) {
                    appName = sanitizeAppName(app);
                    publishFoxtrotEvent(appName, documents, sample);
                    return;
                }
            } catch (Exception ex) {
                errorHandler.onError(appName, documents, ex);
                failedEvents.addAll(eventDocuments.getEvents());
                return;
            }

            errorHandler.onError(app, documents, e);
            log.debug("Adding corresponding events to failed events list : {}", events);
            failedEvents.addAll(events);
        }
    }

    private Map<String, EventDocuments> getAppEventDocumentsMap(Map<String, List<AppDocument>> payloads) {
        Map<String, EventDocuments> appDocumentList = new HashMap<>();
        payloads.forEach(((app, appDocuments) -> {
            List<Event> events = appDocuments.stream().map(AppDocument::getEvent).collect(Collectors.toList());
            List<Document> documents = appDocuments.stream().map(AppDocument::getDocument).collect(Collectors.toList());
            appDocumentList.put(app, EventDocuments.builder()
                    .documents(documents)
                    .events(events)
                    .build());
        }));
        return appDocumentList;
    }

    private Map<String, List<AppDocument>> getValidAppDocuments(EventSet eventSet) {
        AtomicInteger validDocumentCount = new AtomicInteger();
        Map<String, List<AppDocument>> payloads = eventSet.getEvents()
                .stream()
                .map(event -> {

                    // map event data (bytes) to Tree Node
                    JsonNode jsonNode;
                    try {
                        jsonNode = mapper.readTree((byte[]) event.getData());
                    } catch (IOException e) {
                        log.error("Unable to read payload.data as a tree", e);
                        throw new RuntimeException(e);
                    }

                    // filter invalid data
                    if (validNodeFilter.test(jsonNode)) {
                        // map them to AppDocument
                        return AppDocument
                                .builder()
                                .event(event)
                                .app(jsonNode.get(APP_NAME).asText())
                                .document(new Document(jsonNode.get("id").asText(), jsonNode.get("time").asLong(), jsonNode))
                                .build();
                    }
                    return null;
                }).filter(Objects::nonNull)
                .peek(appDocument -> validDocumentCount.getAndIncrement())
                .collect(Collectors.groupingBy(AppDocument::getApp,
                        Collectors.mapping(Function.identity(), Collectors.toList())));

        validEventRateMeter.mark(validDocumentCount.get());

        log.info("Valid {} payloads", validDocumentCount.get());
        return payloads;
    }

    private void publishFoxtrotEvent(String app, List<Document> documents, String sample) throws Exception {
        /* logging a dummy sample data from the list of documents, for debugging purposes */
        log.info("Sending to Foxtrot app:{} size:{} sample:{}",
                app, documents.size(), sample);
        foxtrotClient.send(app, documents);
        log.info("Published to Foxtrot successfully.  app:{} size:{} sample:{}",
                app, documents.size(), sample);
    }

    @Override
    public void destroy() {
        try {
            foxtrotClient.close();
        } catch (Exception e) {
            log.error("Error while closing foxtrot client", e);
        }
    }


    @Data
    @Builder
    @EqualsAndHashCode
    private static class AppDocument {

        private String app;
        private Document document;
        private Event event;
    }

    @Data
    @Builder
    public static class EventDocuments {
        List<Document> documents;
        List<Event> events;
    }
}
