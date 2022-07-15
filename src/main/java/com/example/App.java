package com.example;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.profile.internal.AwsProfileNameLoader;
import com.amazonaws.thirdparty.apache.codec.binary.StringUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args )
    {

        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        System.out.println(schema.toString());

        //https://gist.github.com/hililiwei/ab1b5838740d07abbf59cf37afec0d04
        GlueCatalog catalog = new GlueCatalog();
        Map<String,String> map = new HashMap<>();
        map.put("warehouse","s3://devs3sink/icebergTest/");
        map.put("io-impl","org.apache.iceberg.aws.s3.S3FileIO");
        String name = "test";

        Namespace namespace = Namespace.of("webapp");

        catalog.initialize("test",map);
        Map<String,String> metadata =  new HashMap<>();
        Table table = null;
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "logs");
        if(!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace,metadata);
            table = catalog.createTable(tableIdentifier, schema, null);
        }
        else{
            table = catalog.loadTable(tableIdentifier);
            Transaction t = table.newTransaction();
// commit operations to the transaction
            PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();

            DataFile dataFile = DataFiles.builder(partitionSpec)
                    .withPath("/path/to/data-a.parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .build();
            t.newAppend().appendFile(dataFile).commit();
// commit all the changes to the table
            t.commitTransaction();

        }
        System.out.println(table);
    }
}
