/*
 *  Copyright (c) 2022 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - Initial implementation
 *
 */

package org.eclipse.edc.connector.dataplane.util.sink;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.telemetry.Telemetry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.UUID.randomUUID;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;
import static org.mockito.Mockito.mock;

class StreamedParallelSinkTest {

    private final Monitor monitor = mock(Monitor.class);
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final String dataSourceName = "test-datasource-name";
    private final String errorMessage = "test-errormessage";
    PipedInputStream pipedInputStream = new PipedInputStream();
    private final FakeStreamDataSource dataSource = new FakeStreamDataSource(
            dataSourceName,
            new BufferedInputStream(pipedInputStream));
    private final String dataFlowRequestId = randomUUID().toString();
    FakeParallelSink fakeSink1, fakeSink2;

    @BeforeEach
    void setup() {
        fakeSink1 = new FakeParallelSink();
        fakeSink1.monitor = monitor;
        fakeSink1.telemetry = new Telemetry(); // default noop implementation
        fakeSink1.executorService = executor;
        fakeSink1.requestId = dataFlowRequestId;

        fakeSink2 = new FakeParallelSink();
        fakeSink2.monitor = monitor;
        fakeSink2.telemetry = new Telemetry(); // default noop implementation
        fakeSink2.executorService = executor;
        fakeSink2.requestId = dataFlowRequestId;
    }

    @Test
    void transfer_blocked() throws IOException {
        PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);
        pipedOutputStream.write(new byte[]{'a'});
        System.out.println("Reaches here"); // Reaches here
        var fakeFuture = fakeSink1.transfer(dataSource); //Never returns CompletableFuture
        System.out.println("Doesn't reach here"); // Never reaches here.
        System.out.println(fakeFuture.toString());
        assertThat(fakeSink1.complete).isEqualTo(1);
    }

    private static class FakeParallelSink extends ParallelSink {

        List<DataSource.Part> parts;
        Supplier<StreamResult<Object>> transferResultSupplier = StreamResult::success;
        private int complete;
        private StreamResult<Object> completeResponse = StreamResult.success();

        @Override
        protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
            this.parts = parts;
            return transferResultSupplier.get();
        }

        @Override
        protected StreamResult<Object> complete() {
            complete++;
            return completeResponse;
        }
    }

    public class FakeStreamDataSource implements DataSource, DataSource.Part {
        private final String name;
        private final InputStream stream;

        public FakeStreamDataSource(String name, InputStream stream) {
            this.name = name;
            this.stream = stream;
        }

        @Override
        public StreamResult<Stream<Part>> openPartStream() {
            var stream = openRecordsStream()
                            .map(FakePart::new)
                            .map(Part.class::cast);

            return success(stream);
        }

        @NotNull
        private Stream<byte[]> openRecordsStream() {
            return stream(spliteratorUnknownSize(new FakeIterator(), 0), /* not parallel */ true);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public InputStream openStream() {
            return stream;
        }
    }

    private class FakePart implements DataSource.Part {

        private final byte[] consumerRecord;

        private FakePart(byte[] consumerRecord) {
            this.consumerRecord = consumerRecord;
        }

        @Override
        public String name() {
            return "fakeName";
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(consumerRecord);
        }
    }

    private class FakeIterator implements Iterator<byte[]> {

        private final Instant streamEnd;
        private String nextRecord = "fake";
        private static int i = 0;

        FakeIterator() {
            this.streamEnd = Instant.MAX;
            debug("starts consuming events until: " + streamEnd);
        }

        @Override
        public boolean hasNext() {
            nextRecord = "fake" + i++;
            return true;
        }

        @Override
        public byte[] next() {
            String record = nextRecord;
            if (record == null) {
                throw new NoSuchElementException("No more message");
            }
            nextRecord = null;
            return record.getBytes();
        }

        private void debug(String message) {
            monitor.debug(String.format("MqttDataSource %s", message));
        }

    }
}
