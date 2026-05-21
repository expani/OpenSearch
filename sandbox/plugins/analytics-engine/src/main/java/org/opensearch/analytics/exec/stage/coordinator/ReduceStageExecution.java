/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.SinkProvidingStageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.InputSinkDecorator;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.analytics.spi.ReducingExchangeSink;

import java.util.List;

/**
 * Coordinator-side reduce stage execution. Task invokes {@link ReducingExchangeSink#reduce};
 * {@link #onTerminalTransition} invokes {@link ExchangeSink#close} (idempotent) so
 * cancel-before-reduce paths still release resources. Scheduling mode (eager vs buffered)
 * is delegated to {@link ReducingExchangeSink#supportsEagerScheduling()}.
 *
 * @opensearch.internal
 */
public final class ReduceStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private final ReducingExchangeSink backendSink;
    private final ExchangeSink downstream;
    private final BufferAllocator allocator;

    public ReduceStageExecution(Stage stage, QueryContext config, ReducingExchangeSink backendSink, ExchangeSink downstream) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.allocator = config.bufferAllocator();
        this.runner = new LocalTaskRunner(config.localTaskExecutor());
    }

    @Override
    public boolean schedulesEagerly() {
        return backendSink.supportsEagerScheduling();
    }

    @Override
    public void closeChildInput(int childStageId) {
        if (backendSink instanceof MultiInputExchangeSink multi) {
            multi.sinkForChild(childStageId).close();
        }
    }

    @Override
    public ExchangeSink inputSink(int childStageId) {
        InputSinkDecorator decorator = stage.getInputSinkDecorator();
        if (backendSink instanceof MultiInputExchangeSink multi) {
            // Decorating per-child sub-sinks of a multi-input reducer is not supported.
            // The QTF DAG cut (only producer of decorators today) doesn't fire on
            // multi-input shapes, so this combination is unreachable in practice — the
            // throw is a safety net guarding against future drift.
            if (decorator != null) {
                throw new IllegalStateException(
                    "InputSinkDecorator on a multi-input reducer (stageId=" + getStageId() + ") is not supported"
                );
            }
            return multi.sinkForChild(childStageId);
        }
        return decorator != null ? decorator.decorate(backendSink, allocator) : backendSink;
    }

    @Override
    public ExchangeSource outputSource() {
        if (downstream instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException(
            "downstream sink " + downstream.getClass().getSimpleName() + " does not implement ExchangeSource"
        );
    }

    @Override
    protected List<StageTask> materializeTasks() {
        return List.of(new LocalStageTask(new StageTaskId(getStageId(), 0), backendSink::reduce));
    }

    @Override
    protected void onTerminalTransition(State terminal) {
        try {
            backendSink.close();
        } catch (Exception ignore) {}
    }
}
