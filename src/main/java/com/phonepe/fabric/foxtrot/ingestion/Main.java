package com.phonepe.fabric.foxtrot.ingestion;

import com.phonepe.fabric.foxtrot.ingestion.client.AsyncWorker;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Start time : "+System.currentTimeMillis());
        List<Future<Integer>> collect = IntStream.rangeClosed(1, 100)
                .mapToObj(i -> AsyncWorker.INSTANCE.submit(() -> {
                    log.info("sleeping for 1 second i: {} , thread id : {}", i, Thread.currentThread().getId());
                    Thread.sleep(1000);
                    return i * 2;
                })).collect(Collectors.toList());

        AsyncWorker.INSTANCE.awaitTermination(5 , TimeUnit.MINUTES);

        collect.
        System.out.println("End time : "+System.currentTimeMillis());
    }
}
