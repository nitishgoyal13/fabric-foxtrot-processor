package com.phonepe.fabric.foxtrot.ingestion.errorhandler;

import com.flipkart.foxtrot.client.Document;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public abstract class ErrorHandler {

    @Getter
    private ErrorHandlerType handlerType;

    public ErrorHandler(ErrorHandlerType handlerType) {
        this.handlerType = handlerType;
    }

    public abstract void onError(String app, List<Document> documents, Exception ex);

    public enum ErrorHandlerType {
        MAIN_TOPOLOGY_ERROR_HANDLER, SIDELINE_TOPOLOGY_ERROR_HANDLER
    }
}
