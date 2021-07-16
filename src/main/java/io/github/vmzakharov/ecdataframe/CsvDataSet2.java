package io.github.vmzakharov.ecdataframe.dataset;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 */
public class CsvDataSet2 extends CsvDataSet {

    private Path path;

    public CsvDataSet2(String dataFileName, String newName) {
        super(dataFileName, newName);
    }

    public CsvDataSet2(String dataFileName, String newName, CsvSchema newSchema) {
        super(dataFileName, newName, newSchema);
    }

    public CsvDataSet2(Path dataFileName, String newName) {
        super(null, newName);
        this.path = dataFileName.toAbsolutePath();
    }

    public CsvDataSet2(Path dataFileName, String newName, CsvSchema newSchema) {
        super(null, newName, newSchema);
        this.path = dataFileName.toAbsolutePath();

    }

    @Override
    protected Reader createReader() throws IOException {
        return Files.newBufferedReader(this.path);
    }

    @Override
    protected Writer createWriter() throws IOException {
        return Files.newBufferedWriter(this.path);
    }
}
