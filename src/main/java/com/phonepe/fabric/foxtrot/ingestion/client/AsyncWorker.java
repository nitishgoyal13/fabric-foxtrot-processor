package com.phonepe.fabric.foxtrot.ingestion.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public enum AsyncWorker implements ExecutorService {

    INSTANCE;

    private final ExecutorService worker;

    private AsyncWorker() {
        worker = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() * 4, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("foxtrot-processor-async-worker-%d").build());
    }

    @Override
    public void execute(Runnable command) {
        worker.execute(command);
    }

    @Override
    public void shutdown() {
        worker.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return worker.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return worker.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return worker.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return worker.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return worker.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return worker.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return worker.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return worker.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return worker.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return worker.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return worker.invokeAny(tasks);
    }
}
