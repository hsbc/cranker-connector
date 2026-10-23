package com.hsbc.cranker.connector;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

final class InstrumentationTestLog extends Handler implements AutoCloseable {
    private final Logger logger = Logger.getLogger(ConnectorInstrumentation.class.getName());
    final List<Throwable> failures = new CopyOnWriteArrayList<>();

    InstrumentationTestLog() {
        logger.addHandler(this);
    }

    @Override
    public void publish(LogRecord record) {
        failures.add(record.getThrown());
    }

    @Override
    public void flush() {}

    @Override
    public void close() {
        logger.removeHandler(this);
    }
}
