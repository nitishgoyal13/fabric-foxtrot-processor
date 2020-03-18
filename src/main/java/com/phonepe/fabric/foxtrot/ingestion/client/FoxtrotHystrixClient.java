package com.phonepe.fabric.foxtrot.ingestion.client;

import com.flipkart.foxtrot.client.Document;
import com.flipkart.foxtrot.client.FoxtrotClient;
import com.flipkart.foxtrot.client.FoxtrotClientConfig;
import com.hystrix.configurator.config.*;
import com.hystrix.configurator.core.HystrixConfigurationFactory;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import io.appform.core.hystrix.CommandFactory;
import io.appform.core.hystrix.HandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FoxtrotHystrixClient extends FoxtrotClient {

    public FoxtrotHystrixClient(FoxtrotClientConfig config, int concurrency, int timeout) throws Exception {
        super(config);

        HystrixConfig hystrixConfig = HystrixConfig.builder()
                .defaultConfig(HystrixDefaultConfig.builder()
                        .threadPool(ThreadPoolConfig.builder()
                                .semaphoreIsolation(false)
                                .maxRequestQueueSize(128)
                                .dynamicRequestQueueSize(16)
                                .concurrency(concurrency)
                                .timeout(timeout)
                                .build())
                        .circuitBreaker(new CircuitBreakerConfig())
                        .metrics(new MetricsConfig())
                        .build())
                .commands(new ArrayList<>())
                .build();
        HystrixConfigurationFactory.init(hystrixConfig);
    }

    public void send(Document document) {
        executeInHystrix(() -> {
            super.send(document);
            return true;
        });
    }

    public void send(String tableName, Document document) {
        executeInHystrix(() -> {
            super.send(tableName, document);
            return true;
        });
    }

    public void send(String tableName, List<Document> documents) {
        executeInHystrix(() -> {
            super.send(tableName, documents);
            return true;
        });
    }

    public void send(List<Document> documents) throws Exception {
        executeInHystrix(() -> {
            super.send(documents);
            return true;
        });
    }

    private void executeInHystrix(HandlerAdapter<Boolean> function) {
        try {
            CommandFactory.<Boolean>create(
                    FoxtrotHystrixClient.class.getSimpleName(), "send")
                    .executor(function)
                    .execute();
        } catch (Exception e) {
            log.error("Error while sending documents", e);
            if (e instanceof HystrixRuntimeException && HystrixRuntimeException.FailureType.TIMEOUT
                    .equals(((HystrixRuntimeException) e).getFailureType())) {
                log.error("hystrix command timed out for sending document");
            }
            throw new RuntimeException(e.getMessage());
        }
    }

    public void close() throws Exception {
        super.close();
    }
}
