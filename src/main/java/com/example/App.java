package com.example;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.profile.internal.AwsProfileNameLoader;
import com.amazonaws.thirdparty.apache.codec.binary.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
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
        Catalog catalog = new GlueCatalog();
        Map<String,String> map = new HashMap<>();
        map.put("warehouse","s3://songfuhao-bucket/songfuhao");
        map.put("io-impl","org.apache.iceberg.aws.s3.S3FileIO");
        String name = "test";
        Map<String, String> properties = new HashMap<>();
        catalog.initialize("test",map);



        TableIdentifier name2 = TableIdentifier.of("logging", "logs");
        Table table = catalog.createTable(name2, schema, null);

        System.out.println(table);

    }
}
