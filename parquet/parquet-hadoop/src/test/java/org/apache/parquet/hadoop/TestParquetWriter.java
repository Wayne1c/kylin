/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static java.util.Arrays.asList;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.rules.TemporaryFolder;

public class TestParquetWriter {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestParquetWriter/");
    enforceEmptyDir(conf, root);
    MessageType schema = parseMessageType(
        "message test { "
        + "required binary binary_field; "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required boolean boolean_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required fixed_len_byte_array(3) flba_field; "
        + "required int96 int96_field; "
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    Map<String, Encoding> expected = new HashMap<String, Encoding>();
    expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
    expected.put("1000-" + PARQUET_1_0, PLAIN);
    expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
    expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
    for (int modulo : asList(10, 1000)) {
      for (WriterVersion version : WriterVersion.values()) {
        Path file = new Path(root, version.name() + "_" + modulo);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
            file,
            new GroupWriteSupport(),
            UNCOMPRESSED, 1024, 1024, 512, true, false, version, conf);
        for (int i = 0; i < 1000; i++) {
          writer.write(
              f.newGroup()
              .append("binary_field", "test" + (i % modulo))
              .append("int32_field", 32)
              .append("int64_field", 64l)
              .append("boolean_field", true)
              .append("float_field", 1.0f)
              .append("double_field", 2.0d)
              .append("flba_field", "foo")
              .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
        }
        writer.close();
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
        for (int i = 0; i < 1000; i++) {
          Group group = reader.read();
          assertEquals("test" + (i % modulo), group.getBinary("binary_field", 0).toStringUsingUTF8());
          assertEquals(32, group.getInteger("int32_field", 0));
          assertEquals(64l, group.getLong("int64_field", 0));
          assertEquals(true, group.getBoolean("boolean_field", 0));
          assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
          assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
          assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
          assertEquals(Binary.fromConstantByteArray(new byte[12]),
              group.getInt96("int96_field",0));
        }
        reader.close();
        ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
          for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
            if (column.getPath().toDotString().equals("binary_field")) {
              String key = modulo + "-" + version;
              Encoding expectedEncoding = expected.get(key);
              assertTrue(
                  key + ":" + column.getEncodings() + " should contain " + expectedEncoding,
                  column.getEncodings().contains(expectedEncoding));
            }
          }
        }
        assertEquals("Object model property should be example",
            "example", footer.getFileMetaData().getKeyValueMetaData()
                .get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
      }
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBadWriteSchema() throws IOException {
    final File file = temp.newFile("test.parquet");
    file.delete();

    TestUtils.assertThrows("Should reject a schema with an empty group",
        InvalidSchemaException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            ExampleParquetWriter.builder(new Path(file.toString()))
                .withType(Types.buildMessage()
                    .addField(new GroupType(REQUIRED, "invalid_group"))
                    .named("invalid_message"))
                .build();
            return null;
          }
        });

    Assert.assertFalse("Should not create a file when schema is rejected",
        file.exists());
  }

  @Test
  public void testWrite() throws IOException {
    System.setProperty("HADOOP_USER_NAME", "root");

    MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age").named("Info");


    GroupWriteSupport writeSupport = new GroupWriteSupport();

    Configuration configuration = new Configuration();

    writeSupport.setSchema(schema, configuration);

    GroupFactory factory = new SimpleGroupFactory(schema);

    Path path = new Path("file://///Users/chao.long/IdeaProjects/wayne/kylin/parquet-hadoop/src/main/java/org/apache/parquet/test.parquet");
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    fileSystem.delete(path, true);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);

    BufferedReader localReader = new BufferedReader(new FileReader("/Users/chao.long/IdeaProjects/wayne/kylin/parquet-hadoop/src/main/java/org/apache/parquet/test.txt"));

    String line;
    while ((line = localReader.readLine()) != null) {
      String[] splited = line.split(",");
      Group group = factory.newGroup();
      group.append("name", splited[0])
              .append("age", Integer.valueOf(splited[1]));


      writer.write(group);
    }

    writer.close();
  }

  @Test
  public void testRead() throws IOException {
    Path path = new Path("file://///Users/chao.long/IdeaProjects/wayne/kylin/parquet-hadoop/src/main/java/org/apache/parquet/test.parquet");

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, path);
    builder.useColumnIndexFilter();
    builder.useStatsFilter();
    builder.useDictionaryFilter();
    builder.useRecordFilter();

    builder.withFilter(FilterCompat.get(gt(intColumn("age"), 3)));
    ParquetReader<Group> reader = builder.build();

    Group group;

    while ((group = reader.read()) != null) {
      System.out.println("name=" + group.getString("name", 0) + ", age=" + group.getInteger("age", 0));
    }

  }

  @Test
  public void testReadIndex() throws IOException {
    Configuration configuration = new Configuration();

    MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age").named("Info");


    Path path = new Path("file://///Users/chao.long/IdeaProjects/wayne/kylin/parquet-hadoop/src/main/java/org/apache/parquet/test.parquet");
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, path, NO_FILTER);

    ParquetFileReader reader = new ParquetFileReader(
            configuration, footer.getFileMetaData(), path, footer.getBlocks(), schema.getColumns());

    ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);
    ColumnIndex columnIndex = reader.readColumnIndex(column);

  }

  @Test
  public void testWrite1() throws IOException {
    System.setProperty("HADOOP_USER_NAME", "root");

    MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age").named("Info");


    GroupWriteSupport writeSupport = new GroupWriteSupport();

    Configuration configuration = new Configuration();

    writeSupport.setSchema(schema, configuration);

    GroupFactory factory = new SimpleGroupFactory(schema);

    Path path = new Path("file://///Users/chao.long/IdeaProjects/wayne/kylin/parquet-hadoop/src/main/java/org/apache/parquet/test.parquet");
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    fileSystem.delete(path, true);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);

    BufferedReader localReader = new BufferedReader(new FileReader("/Users/chao.long/IdeaProjects/wayne/kylin/parquet-hadoop/src/main/java/org/apache/parquet/test.txt"));

    String line;
    while ((line = localReader.readLine()) != null) {
      String[] splited = line.split(",");
      Group group = factory.newGroup();
      group.append("name", splited[0]);
      group.append("name", "11");
      group.append("age", Integer.valueOf(splited[1]));


      writer.write(group);
    }

    writer.close();
  }

  @Test
  public void testRead1() throws IOException {
    String file = "hdfs://10.1.2.121:8020/kylin/kylin_metadata_30/kylin-ef8b2859-e100-52ce-6524-2a88044c676f/kylin_sales_cube_spark/cuboid/level_2_cuboid/31999-part-r-00005.parquet";
    Path path = new Path(file);//"file://////Users/chao.long/Documents/data/24575-part-r-00004.parquet");

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, path);
    builder.useColumnIndexFilter();
    builder.useStatsFilter();
    builder.useDictionaryFilter();
    builder.useRecordFilter();

    Operators.LongColumn date = longColumn("KYLIN_SALES.PART_DT");
    Operators.IntColumn cname = intColumn("BUYER_COUNTRY.NAME");

    FilterPredicate pred = gt(date, 1388361600000L);
    pred = eq(cname, 3);

    builder.withFilter(FilterCompat.get(pred));
    ParquetReader<Group> reader = builder.build();

    Group group;
    long count = 0;
    while ((group = reader.read()) != null) {
      System.out.println(group.toString());
      count ++;
    }

    System.out.println("count = " + count);

  }

  @Test
  public void testReadIndex1() throws IOException {

    Configuration configuration = new Configuration();

    MessageType schema = MessageTypeParser.parseMessageType("message 21503 {\n" +
            "\trequired int64 KYLIN_SALES.PART_DT;\n" +
            "\trequired int32 KYLIN_CATEGORY_GROUPINGS.META_CATEG_NAME;\n" +
            "\trequired int32 BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL;\n" +
            "\trequired int32 SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL;\n" +
            "\trequired int32 BUYER_ACCOUNT.ACCOUNT_COUNTRY;\n" +
            "\trequired int32 SELLER_ACCOUNT.ACCOUNT_COUNTRY;\n" +
            "\trequired int32 BUYER_COUNTRY.NAME;\n" +
            "\trequired int32 SELLER_COUNTRY.NAME;\n" +
            "\trequired int32 KYLIN_SALES.LSTG_FORMAT_NAME;\n" +
            "\trequired int32 KYLIN_SALES.LSTG_SITE_ID;\n" +
            "\trequired int32 KYLIN_SALES.OPS_USER_ID;\n" +
            "\trequired int32 KYLIN_SALES.OPS_REGION;\n" +
            "\trequired binary GMV_SUM (DECIMAL(19,4));\n" +
            "\trequired int64 BUYER_LEVEL_SUM;\n" +
            "\trequired int64 SELLER_LEVEL_SUM;\n" +
            "\trequired int64 TRANS_CNT;\n" +
            "\trequired binary SELLER_CNT_HLL;\n" +
            "\trequired binary TOP_SELLER;\n" +
            "\trequired double SUM_L;\n" +
            "}");


    Path path = new Path("file://////Users/chao.long/Documents/data/part-r-00000.parquet");
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, path, NO_FILTER);

    ParquetFileReader reader = new ParquetFileReader(
            configuration, footer.getFileMetaData(), path, footer.getBlocks(), schema.getColumns());

    ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(1);
    ColumnIndex columnIndex = reader.readColumnIndex(column);

  }
}
