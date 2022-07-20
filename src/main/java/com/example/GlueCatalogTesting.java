package com.example;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.utils.ImmutableMap;

import org.apache.iceberg.*;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.catalog.TableIdentifier;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.s3.S3Client;

public class GlueCatalogTesting {

    private static final String S3 = "s3://";

    private GlueCatalog glueCatalog;

    private List<Record> records;

    // aws clients
    final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
    final GlueClient glue = clientFactory.glue();
    final S3Client s3 = clientFactory.s3();

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "binary", Types.StringType.get()));


    //private PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();

    public GlueCatalogTesting() {
        glueCatalog = new GlueCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://icebergtestbhond/");
        String catalogName = "glue";
        glueCatalog.initialize(catalogName, properties);
    }

    public void createTableAndAppend() throws IOException {
      /*  String namespace = createNamespace();
        String tableName = getTableName();
        Table iceBergTable = glueCatalog.createTable(TableIdentifier.of(namespace, tableName), SCHEMA);*/
        // verify table exists in Glue
        Table iceBergTable = glueCatalog.loadTable(TableIdentifier.of("webapps", "iceberg_table"));

        System.out.println(iceBergTable);

        //GetTableResponse response = glue.getTable(GetTableRequest.builder().databaseName('webapp').name('iceberg_table').build());

        //Assert.assertEquals(namespace, response.table().databaseName());
        //Assert.assertEquals(tableName, response.table().name());
       // Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH), response.table().parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
        //Assert.assertTrue(response.table().parameters().containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
       // Assert.assertEquals(SCHEMA.columns().size(), response.table().storageDescriptor().columns().size());
         DataFile recordsData = createRecords();
         insert(iceBergTable, recordsData);
    }

    private void insert(Table iceBergTable, DataFile recordsData) {
        iceBergTable.newAppend().appendFile(recordsData).commit();
    }

    public DataFile createRecords() throws IOException {
        GenericRecord genericRecord = GenericRecord.create(SCHEMA);
        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        builder.add(genericRecord.copy(ImmutableMap.of("id", 1L, "data", "a")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 2L, "data", "b")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 3L, "data", "c")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 4L, "data", "d")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 5L, "data", "e")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 6L, "data", "e")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 7L, "data", "e")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 8L, "data", "e")));
        builder.add(genericRecord.copy(ImmutableMap.of("id", 9L, "data", "e")));

        this.records = builder.build();

        File temp = new File("/tmp/bhanu");
        OutputFile file = Files.localOutput(temp);

        DataWriter<Record> dataWriter = Parquet.writeData(file)
                .schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .withSpec(PartitionSpec.unpartitioned()).build();

        try {
            for (Record rec : records) {
                dataWriter.write(rec);
            }
        } finally {
            dataWriter.close();
        }

        return dataWriter.toDataFile();
    }


    private String getTableName() {
        return "vankaitest";
    }

    public String createNamespace() {
        String namespace = "webapps";
        this.glueCatalog.createNamespace(Namespace.of(namespace));
        return namespace;
    }


}
