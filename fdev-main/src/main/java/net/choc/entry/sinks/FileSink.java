package net.choc.entry.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileSink extends RichSinkFunction<String> {
    private FileOutputStream outputStream;

    @Override
    public void open(Configuration parameters) throws FileNotFoundException {
        File file = new File("test.log");
        outputStream = new FileOutputStream(file, true);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String line = String.format("%s%n", value);
        outputStream.write(line.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
    }
}
