package com.phonepe.fabric.foxtrot.ingestion.errorhandler;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.FoxtrotClient;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.phonepe.fabric.foxtrot.ingestion.errorhandler.ErrorHandler.ErrorHandlerType;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
@Data
@Builder
public class ErrorHandlerFactory {

    private String s;
    private Properties global;
    private Properties local;
    private ComponentMetadata componentMetadata;
    private FoxtrotClient foxtrotClient;
    private ObjectMapper mapper;

    public ErrorHandlerFactory(String s, Properties global, Properties local, ComponentMetadata componentMetadata,
                               FoxtrotClient foxtrotClient, ObjectMapper mapper) {
        this.s = s;
        this.global = global;
        this.local = local;
        this.componentMetadata = componentMetadata;
        this.foxtrotClient = foxtrotClient;
        this.mapper = mapper;
    }

    public ErrorHandler get(ErrorHandlerType handlerType) {
        switch (handlerType) {
            case MAIN_TOPOLOGY_ERROR_HANDLER:
                return new MainTopologyErrorHandler();
            case SIDELINE_TOPOLOGY_ERROR_HANDLER:
                return new SidelineTopologyErrorHandler(s, global, local, componentMetadata, foxtrotClient, mapper);
            default:
                log.error("Unsupported error handler type : {}", handlerType);
                throw new RuntimeException("Unsupported error handler type");
        }
    }
}