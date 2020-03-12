package com.phonepe.fabric.foxtrot.ingestion.errorhandler;

import com.flipkart.foxtrot.client.Document;
import com.phonepe.fabric.foxtrot.ingestion.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MainTopologyErrorHandler extends ErrorHandler {

    /*
        Do nothing for exceptions in main topology, just log the error
     */
    @Override
    public void onError(String app, List<Document> documents, Exception ex) {
        String sampleDocument = Utils.getSampleDocument(documents);
        log.error("Failed to send document list:" + app
                + " size:" + documents.size() + " sample:" + sampleDocument, ex);
    }
}