package org.dancres.peers.primitives;

import org.slf4j.Logger;

import java.io.IOException;
import java.io.Writer;

public class LoggerWriter extends Writer {
    private Logger _logger;

    public LoggerWriter(Logger aLogger) {
        _logger = aLogger;
    }

    public void write(char[] cbuf, int off, int len) throws IOException {
        String myOutput = new String(cbuf, off, len);
        _logger.info(myOutput);
    }

    public void flush() throws IOException {
    }

    public void close() throws IOException {
    }
}
