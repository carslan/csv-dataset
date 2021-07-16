package io.github.vmzakharov.ecdataframe.dataset;

import com.google.common.jimfs.Jimfs;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvDataSet2Test
{
    private static final String FILENAME = "dataset.csv";
    private static final String ROOT_DIR = "src" + File.separator + "test" + File.separator + "resources" + File.separator;

    @Test
    void loadDataFrame()
    {
        final DataFrame dataFrame = new CsvDataSet2(this.filePathOnFilesystem(), "example").loadAsDataFrame();

        this.assertDataFrame(dataFrame);
    }

    @Test
    void loadDataFrameFromVirtualFilesystem() throws IOException
    {
        final DataFrame dataFrame = new CsvDataSet2(this.memoryPath(), "example-2").loadAsDataFrame();

        this.assertDataFrame(dataFrame);
    }

    @Test
    void loadDataFrameWithSchema()
    {
        final DataFrame dataFrame = new CsvDataSet2(this.filePathOnFilesystem(), "example-3", this.schema()).loadAsDataFrame();

        this.assertDataFrame(dataFrame);
    }

    @Test
    void loadDataFrameWithSchemaFromVirtualFilesystem() throws IOException
    {
        final DataFrame dataFrame = new CsvDataSet2(this.memoryPath(), "example-4", this.schema()).loadAsDataFrame();

        this.assertDataFrame(dataFrame);
    }

    @Test
    void writeToFilesystem()
    {
        final String name = "example-5";
        final DataFrame df = new CsvDataSet2(this.filePathOnFilesystem(), name).loadAsDataFrame();
        final Path fullFilePath = Paths.get(ROOT_DIR + name + ".csv");
        new CsvDataSet2(fullFilePath, name).write(df);

        assertTrue(Files.exists(fullFilePath));
    }

    @Test
    void writeToMemory() throws IOException
    {
        final String name = "example-6";
        final Path parentDir = this.memoryPath().getParent();
        final DataFrame df = new CsvDataSet2(this.memoryPath(), name).loadAsDataFrame();
        final Path fullFilePath = parentDir.resolve(name + ".csv");
        new CsvDataSet2(fullFilePath, name).write(df);

        assertTrue(Files.exists(fullFilePath));
    }

    private CsvSchema schema()
    {
        final CsvSchema csvSchema = new CsvSchema()
                .nullMarker(null)
                .separator(',')
                .quoteCharacter('"');
        csvSchema.addColumn("ID", ValueType.LONG);
        csvSchema.addColumn("Account", ValueType.STRING);
        csvSchema.addColumn("Security", ValueType.STRING);
        csvSchema.addColumn("Glorp", ValueType.STRING);
        csvSchema.addColumn("Quantity", ValueType.LONG);
        csvSchema.addColumn("MarketValue", ValueType.DOUBLE);

        return csvSchema;
    }

    private Path filePathOnFilesystem()
    {
        return Paths.get(ROOT_DIR + FILENAME);
    }

    private Path memoryPath() throws IOException
    {
        final Path filePathOnFilesystem = Paths.get(ROOT_DIR + FILENAME).toAbsolutePath();

        final FileSystem fileSystem = Jimfs.newFileSystem();
        final Path rootDir = fileSystem.getPath(filePathOnFilesystem.getParent().toString());
        Files.createDirectories(rootDir);
        final Path memoryPath = rootDir.resolve(FILENAME);
        Files.copy(filePathOnFilesystem, memoryPath);
        assertTrue(Files.exists(memoryPath));

        return memoryPath;
    }

    private void assertDataFrame(final DataFrame df)
    {
        assertEquals(7, df.rowCount());
        assertEquals(6, df.getColumns().size());
        assertEquals("ID", df.getColumnAt(0).getName());
        assertEquals(ValueType.LONG, df.getColumnAt(0).getType());
        long[] ids = {1, 2, 3, 4, 5, 6, 7, 0, 0, 0};
        Lists
                .immutable
                .of(ids)
                .forEachWithIndex((id, index) -> assertEquals(id[index], df.getColumnAt(0).getObject(index)));

        assertEquals("Account", df.getColumnAt(1).getName());
        assertEquals(ValueType.STRING, df.getColumnAt(1).getType());
        String[] accts = {"1234", "1234", "4567", "4567", "4567", "5678", "5678"};
        Lists
                .immutable
                .of(accts)
                .forEachWithIndex((acc, index) -> assertEquals(acc, df.getColumnAt(1).getValueAsString(index)));

        assertEquals("Security", df.getColumnAt(2).getName());
        assertEquals(ValueType.STRING, df.getColumnAt(2).getType());
        String[] sec = {"XXX", "XXX", "AAAA", "AAAA", "CCCC", "BBB", "MMMM"};
        Lists
                .immutable
                .of(sec)
                .forEachWithIndex((id, index) -> assertEquals(id, df.getColumnAt(2).getValueAsString(index)));

        assertEquals("Glorp", df.getColumnAt(3).getName());
        assertEquals(ValueType.STRING, df.getColumnAt(3).getType());
        String[] glorp = {"AA", "BB", "CC", "CC", "DD", "EE", "EE"};
        Lists
                .immutable
                .of(glorp)
                .forEachWithIndex((id, index) -> assertEquals(id, df.getColumnAt(3).getValueAsString(index)));

        assertEquals("Quantity", df.getColumnAt(4).getName());
        assertEquals(ValueType.LONG, df.getColumnAt(4).getType());
        long[] qty = {11, 11, 20, 20, 26, 2, 12};
        Lists
                .immutable
                .of(qty)
                .forEachWithIndex((id, index) -> assertEquals(id[index], (long) df.getColumnAt(4).getObject(index)));

        assertEquals("MarketValue", df.getColumnAt(5).getName());
        assertEquals(ValueType.DOUBLE, df.getColumnAt(5).getType());
        double[] mv = {11.0, 11.0, 20.0, 20.0, 25.0, 2.0, 12.5};
        Lists
                .immutable
                .of(mv)
                .forEachWithIndex((id, index) -> assertEquals(id[index], (double) df.getColumnAt(5).getObject(index)));
    }
}
