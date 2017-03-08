package com.phonepe.fabric.foxtrot.ingestion;

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
import com.olacabs.fabric.model.event.EventSet;
import com.olacabs.fabric.model.processor.Processor;
import com.olacabs.fabric.model.processor.ProcessorType;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A Processor implementation that publishes events to foxtrot
 */
@Processor(namespace = "global",
        name = "foxtrot-processor",
        version = "0.1",
        cpu = 0.1,
        memory = 32,
        description = "A processor that publishes events to Foxtrot",
        processorType = ProcessorType.EVENT_DRIVEN,
        requiredProperties = {"foxtrot.host", "foxtrot.port"},
        optionalProperties = {})
@Slf4j
public class FoxtrotProcessor extends StreamingProcessor {

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
            log.error(String.format("Error creating foxtrot client with hosts%s, port:%s", foxtrotHost, foxtrotPort), e);
            throw new RuntimeException(e);
        }
        mapper = new ObjectMapper();
    }

    @Override
    protected EventSet consume(ProcessingContext processingContext, EventSet eventSet) throws ProcessingException {

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
                .filter(Objects::nonNull)
                .filter(node -> node.has("id") && node.has("time") && node.has("app"))
                .map(node -> AppDocuments
                        .builder()
                        .app(node.get("app").asText())
                        .document(new Document(node.get("id").asText(), node.get("time").asLong(), node))
                        .build())
                .collect(Collectors.groupingBy(AppDocuments::getApp,
                        Collectors.mapping(AppDocuments::getDocument, Collectors.toList())));

        log.info("Received {} payloads", eventSet.getEvents().size());
        payloads.entrySet()
                .forEach(k -> log.info(k.getKey() + ":" + k.getValue().size()));

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
                                + " size:" + documents.size() + " sample:" + documents.stream()
                                .map(d -> d.getData().toString())
                                .collect(Collectors.toList()), e);
                        throw new RuntimeException(e);
                    }
                });
        return null;
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
    static class AppDocuments {
        private String app;
        private Document document;
    }
}
