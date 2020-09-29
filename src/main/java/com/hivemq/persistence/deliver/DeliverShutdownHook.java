package com.hivemq.persistence.deliver;

import com.hivemq.common.shutdown.HiveMQShutdownHook;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DeliverShutdownHook extends HiveMQShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(DeliverShutdownHook.class);

    private ScheduledExecutorService scheduledExecutorService;

    public DeliverShutdownHook(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public @NotNull String name() {
        return "Deliver Shutdown";
    }

    @Override
    public @NotNull Priority priority() {
        return Priority.FIRST;
    }

    @Override
    public boolean isAsynchronous() {
        return false;
    }

    @Override
    public void run() {
        log.info("Waiting for deliver schedule shutdown");
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(20, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
                log.warn("Termination of deliver schedule service timed out after {} seconds. Enforcing shutdown.", 20);
            } else {
                log.info("Graceful shutdown deliver schedule");
            }
        } catch (final InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            log.warn("Not able to wait for deliver schedule service shutdown. Enforcing shutdown.", e);
        }
    }
}
