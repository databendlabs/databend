package org.example;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DeleteRecord {
    Schema schema;
    Record record;
    String fieldName;

    public DeleteRecord(Schema schema, Record record, String fieldName) {
        this.schema = schema;
        this.record = record;
        this.fieldName = fieldName;
    }

    int fieldId() {
        return this.schema.findField(this.fieldName).fieldId();
    }
}

public class Driver {

    public static void main(String[] args) throws SQLException, NoSuchTableException, IOException {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Iceberg REST Catalog").master("local[*]")
                // Iceberg REST Catalog
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type", "rest")
                .config("spark.sql.catalog.iceberg.uri", "http://127.0.0.1:8181")
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-tpch/")
                .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
                .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password")
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
                .config("spark.sql.catalog.iceberg.s3.endpoint", "http://127.0.0.1:9002")
                .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
                .config("spark.jars.packages",
                        "org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")

                .getOrCreate();

        spark.sql("CREATE OR REPLACE TABLE iceberg.test.test_merge_on_read_deletes (\n" +
                "    dt     date,\n" +
                "    number integer,\n" +
                "    letter string\n" +
                ")\n" +
                "USING iceberg\n" +
                "TBLPROPERTIES (\n" +
                "    'write.delete.mode'='merge-on-read',\n" +
                "    'write.update.mode'='merge-on-read',\n" +
                "    'write.merge.mode'='merge-on-read',\n" +
                "    'format-version'='2'\n" +
                ");");
        spark.sql("INSERT INTO iceberg.test.test_merge_on_read_deletes\n" +
                "VALUES\n" +
                "    (CAST('2023-03-01' AS date), 1, 'a'),\n" +
                "    (CAST('2023-03-02' AS date), 2, 'b'),\n" +
                "    (CAST('2023-03-03' AS date), 3, 'c'),\n" +
                "    (CAST('2023-03-04' AS date), 4, 'd'),\n" +
                "    (CAST('2023-03-05' AS date), 5, 'e'),\n" +
                "    (CAST('2023-03-06' AS date), 6, 'f'),\n" +
                "    (CAST('2023-03-07' AS date), 7, 'g'),\n" +
                "    (CAST('2023-03-08' AS date), 8, 'h'),\n" +
                "    (CAST('2023-03-09' AS date), 9, 'i'),\n" +
                "    (CAST('2023-03-10' AS date), 10, 'j'),\n" +
                "    (CAST('2023-03-11' AS date), 11, 'k'),\n" +
                "    (CAST('2023-03-12' AS date), 12, 'l');");
        spark.sql("DELETE FROM iceberg.test.test_merge_on_read_deletes WHERE number > 4 AND number < 7");
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "http://127.0.0.1:8181");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://iceberg-tpch/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.access-key-id", "admin");
        properties.put("s3.secret-access-key", "password");
        properties.put("s3.endpoint", "http://127.0.0.1:9002");
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");

        RESTCatalog restCatalog = new RESTCatalog();
        restCatalog.initialize("rest", properties);

        TableIdentifier tableId = TableIdentifier.of("test", "test_merge_on_read_deletes");
        Table table = restCatalog.loadTable(tableId);
        System.out.println("Loaded table: " + table.name());

        RowDelta rowDelta = table.newRowDelta();
        PartitionSpec spec = table.spec();

        List<DeleteRecord> deleteRecords = new ArrayList<>();

        Schema number = table.schema().select("number");
        Record deleteRecord0 = GenericRecord.create(number);
        deleteRecord0.setField("number", 3);
        deleteRecords.add(new DeleteRecord(number, deleteRecord0, "number"));

        Schema letter = table.schema().select("letter");
        Record deleteRecord1 = GenericRecord.create(letter);
        deleteRecord1.setField("letter", "k");
        deleteRecords.add(new DeleteRecord(letter, deleteRecord1, "letter"));

        for (int i = 0; i < deleteRecords.size(); i++) {
            String deleteFilePath = "s3://iceberg-tpch/test/test_merge_on_read_deletes/data/equality-delete-file-" + i + ".parquet";
            OutputFile outputFile = table.io().newOutputFile(deleteFilePath);

            FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(deleteRecords.get(i).schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

            appender.add(deleteRecords.get(i).record);
            appender.close();

            PartitionData partitionData = new PartitionData(table.spec().partitionType());

            DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec)
                    .ofEqualityDeletes(deleteRecords.get(i).fieldId())
                    .withFormat(FileFormat.PARQUET)
                    .withPath(deleteFilePath)
                    .withPartition(partitionData)
                    .withFileSizeInBytes(appender.length())
                    .withMetrics(appender.metrics())
                    .withSplitOffsets(appender.splitOffsets())
                    .withSortOrder(table.sortOrder())
                    .build();

            rowDelta.addDeletes(deleteFile);
        }
        rowDelta.commit();

        spark.sql("INSERT INTO iceberg.test.test_merge_on_read_deletes VALUES (CAST('2023-03-30' AS date), 6, 'z');");
    }
}
