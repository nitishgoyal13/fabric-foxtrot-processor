package com.phonepe.fabric.foxtrot.ingestion.errorhandler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.FoxtrotClient;
import com.olacabs.fabric.compute.util.ComponentPropertyReader;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.phonepe.fabric.foxtrot.ingestion.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.phonepe.fabric.foxtrot.ingestion.FoxtrotProcessor.APP_NAME;

@Slf4j
public class SidelineTopologyErrorHandler extends ErrorHandler {

    private static final String ERROR = "ingestionException";
    private static final String ERROR_MESSAGE = "ingestionExceptionMessage";

    private String errorTableName;
    private FoxtrotClient foxtrotClient;
    private ObjectMapper mapper;

    SidelineTopologyErrorHandler(String s, Properties global, Properties local,
                                 ComponentMetadata componentMetadata, FoxtrotClient foxtrotClient,
                                 ObjectMapper mapper) {
        this.errorTableName = ComponentPropertyReader.readString(local, global,
                "errorTable", s, componentMetadata, "debug");
        this.foxtrotClient = foxtrotClient;
        this.mapper = mapper;
    }

    @Override
    public void onError(String app, List<Document> documents, Exception ex) {
        String sampleDocument = Utils.getSampleDocument(documents);
        log.error("Failed to send document list:" + app
                + " size:" + documents.size() + " sample:" + sampleDocument, ex);
        ingestFailedDocuments(app, documents, sampleDocument, ex);

        throw new RuntimeException(ex);
    }

    private void ingestFailedDocuments(String app, List<Document> documents, String sampleDocument, Exception exception) {
        try {
            List<Document> failedDocuments = new ArrayList<>();
            for (Document document : documents) {
                Map<String, Object> data = readMapFromObject(document.getData());
                data.put(ERROR, exception.getClass().getName());
                data.put(ERROR_MESSAGE, exception.getMessage());
                data.put(APP_NAME, app);
                document.setData(mapper.valueToTree(data));
                failedDocuments.add(document);
            }
            foxtrotClient.send(errorTableName, failedDocuments);
            log.info("Successfully sent failed documents to debug table for exception :{}, {}",
                    exception.getClass().getName(), exception.getMessage());
        } catch (Exception ex) {
            log.error("Error sending failed document list:" + app
                    + " size:" + documents.size() + " sample:" + sampleDocument, ex);
        }
    }


    private Map<String, Object> readMapFromObject(Object obj) {
        return mapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
        });
    }
}

