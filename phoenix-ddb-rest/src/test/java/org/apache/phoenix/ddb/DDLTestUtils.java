/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.ddb;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.junit.Assert;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Utility functions for DDL API tests.
 */
public class DDLTestUtils {

    /**
     * Return a CreateTableRequest with the given table name and key schema.
     * @return
     */
    public static CreateTableRequest getCreateTableRequest(String tableName, String hashKey,
               ScalarAttributeType hashKeyType, String sortKey, ScalarAttributeType sortKeyType) {
        CreateTableRequest.Builder createTableRequest = CreateTableRequest.builder();
        createTableRequest.tableName(tableName);
        List<AttributeDefinition> attrDefs = new ArrayList<>();
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build());
        attrDefs.add(AttributeDefinition.builder().attributeName(hashKey).attributeType(hashKeyType).build());
        if (sortKey != null && sortKeyType != null) {
            keySchemaElements.add(KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build());
            attrDefs.add(AttributeDefinition.builder().attributeName(sortKey).attributeType(sortKeyType).build());
        }
        createTableRequest.keySchema(keySchemaElements);
        createTableRequest.attributeDefinitions(attrDefs);
        createTableRequest.provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(50L).writeCapacityUnits(50L).build());
        return createTableRequest.build();
    }

    /**
     * Add index to the given CreateTableRequest with the given key schema.
     */
    public static CreateTableRequest addIndexToRequest(boolean isGlobalIndex, CreateTableRequest request, String indexName, String hashKey,
               ScalarAttributeType hashKeyType, String sortKey, ScalarAttributeType sortKeyType) {
        CreateTableRequest.Builder requestBuilder = request.toBuilder();
        List<KeySchemaElement> idxKeySchemaElements = new ArrayList<>();
        List<AttributeDefinition> attrDefs = new ArrayList<>(request.attributeDefinitions());
        idxKeySchemaElements.add(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build());
        // for local index, hash key will be the same as the table
        // and would already be present in the request attributes
        if (isGlobalIndex) {
            attrDefs.add(AttributeDefinition.builder().attributeName(hashKey).attributeType(hashKeyType).build());
        }
        if (sortKey != null && sortKeyType != null) {
            idxKeySchemaElements.add(KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build());
            attrDefs.add(AttributeDefinition.builder().attributeName(sortKey).attributeType(sortKeyType).build());
        }
        if (isGlobalIndex) {
            requestBuilder = addGlobalIndexToRequest(requestBuilder, indexName, idxKeySchemaElements, request.globalSecondaryIndexes());
        } else {
            requestBuilder = addLocalIndexToRequest(requestBuilder, indexName, idxKeySchemaElements, request.localSecondaryIndexes());
        }
        return requestBuilder.attributeDefinitions(attrDefs).build();
    }

    /**
     * Add StreamSpecification to the given CreateTableRequest with the given stream type.
     */
    public static CreateTableRequest addStreamSpecToRequest(CreateTableRequest request, String streamType) {
        StreamSpecification streamSpec
                = StreamSpecification.builder().streamEnabled(true).streamViewType(streamType).build();
        return request.toBuilder().streamSpecification(streamSpec).build();
    }

    /**
     * Verify table name, key schema, attributes and indexes are same in the provided TableDescriptions
     * @throws Exception
     */
    public static void assertTableDescriptions(TableDescription tableDescription1,
                                         TableDescription tableDescription2) throws Exception {
        // table name
        Assert.assertEquals(tableDescription1.tableName(), tableDescription2.tableName());
        Assert.assertEquals(tableDescription1.tableStatus(), tableDescription2.tableStatus());


        // key schema
        List<KeySchemaElement> keySchemaElements1 = tableDescription1.keySchema();
        List<KeySchemaElement> keySchemaElements2 = tableDescription2.keySchema();

        Assert.assertEquals(keySchemaElements1.size(), keySchemaElements2.size());
        for (int i = 0; i < keySchemaElements1.size(); i++) {
            Assert.assertEquals(keySchemaElements1.get(i).attributeName(),
                    keySchemaElements2.get(i).attributeName());
            Assert.assertEquals(keySchemaElements1.get(i).keyType(),
                    keySchemaElements2.get(i).keyType());
        }

        // attribute definitions
        List<AttributeDefinition> attributeDefinitions1 =
                tableDescription1.attributeDefinitions();
        List<AttributeDefinition> attributeDefinitions2 =
                tableDescription2.attributeDefinitions();

        Assert.assertEquals(attributeDefinitions1.size(), attributeDefinitions2.size());
        for (int i = 0; i < attributeDefinitions1.size(); i++) {
            Assert.assertEquals(attributeDefinitions1.get(i).attributeName(),
                    attributeDefinitions2.get(i).attributeName());
            Assert.assertEquals(attributeDefinitions1.get(i).attributeType(),
                    attributeDefinitions2.get(i).attributeType());
        }

        // global indexes
        List<GlobalSecondaryIndexDescription> indexDescriptions1 =
                tableDescription1.globalSecondaryIndexes();
        List<GlobalSecondaryIndexDescription> indexDescriptions2 =
                tableDescription2.globalSecondaryIndexes();
        Assert.assertTrue((indexDescriptions1==null && indexDescriptions2==null)
                || (indexDescriptions1!=null && indexDescriptions2!=null));

        if (indexDescriptions1 != null) {
            Assert.assertEquals("Global index count mismatch", indexDescriptions1.size(),
                    indexDescriptions2.size());

            Map<String, GlobalSecondaryIndexDescription> indexMap2 = new HashMap<>();
            for (GlobalSecondaryIndexDescription idx : indexDescriptions2) {
                indexMap2.put(idx.indexName(), idx);
            }

            for (GlobalSecondaryIndexDescription idx1 : indexDescriptions1) {
                String indexName = idx1.indexName();
                Assert.assertTrue("Index " + indexName + " not found in second description",
                        indexMap2.containsKey(indexName));

                GlobalSecondaryIndexDescription idx2 = indexMap2.get(indexName);
                Assert.assertEquals("Index status mismatch for " + indexName, idx1.indexStatus(),
                        idx2.indexStatus());
                Assert.assertEquals("Key schema size mismatch for index " + indexName,
                        idx1.keySchema().size(), idx2.keySchema().size());

                Map<KeyType, KeySchemaElement> keyMap1 = new HashMap<>();
                Map<KeyType, KeySchemaElement> keyMap2 = new HashMap<>();
                for (KeySchemaElement key : idx1.keySchema()) {
                    keyMap1.put(key.keyType(), key);
                }
                for (KeySchemaElement key : idx2.keySchema()) {
                    keyMap2.put(key.keyType(), key);
                }

                for (KeyType keyType : keyMap1.keySet()) {
                    Assert.assertTrue("Key type " + keyType + " not found in index " + indexName,
                            keyMap2.containsKey(keyType));
                    Assert.assertEquals(
                            "Attribute name mismatch for " + keyType + " key in index " + indexName,
                            keyMap1.get(keyType).attributeName(),
                            keyMap2.get(keyType).attributeName());
                }
            }
        }

        // local indexes
        List<LocalSecondaryIndexDescription> localIndexDescriptions1 =
                tableDescription1.localSecondaryIndexes();
        List<LocalSecondaryIndexDescription> localIndexDescriptions2 =
                tableDescription2.localSecondaryIndexes();
        Assert.assertTrue((localIndexDescriptions1 == null && localIndexDescriptions2 == null) || (
                localIndexDescriptions1 != null && localIndexDescriptions2 != null));

        if (localIndexDescriptions1 != null) {
            Assert.assertEquals("Local index count mismatch", localIndexDescriptions1.size(),
                    localIndexDescriptions2.size());

            Map<String, LocalSecondaryIndexDescription> localIndexMap2 = new HashMap<>();
            for (LocalSecondaryIndexDescription idx : localIndexDescriptions2) {
                localIndexMap2.put(idx.indexName(), idx);
            }

            for (LocalSecondaryIndexDescription idx1 : localIndexDescriptions1) {
                String indexName = idx1.indexName();
                Assert.assertTrue("Local index " + indexName + " not found in second description",
                        localIndexMap2.containsKey(indexName));

                LocalSecondaryIndexDescription idx2 = localIndexMap2.get(indexName);
                Assert.assertEquals("Key schema size mismatch for local index " + indexName,
                        idx1.keySchema().size(), idx2.keySchema().size());

                Map<KeyType, KeySchemaElement> keyMap1 = new HashMap<>();
                Map<KeyType, KeySchemaElement> keyMap2 = new HashMap<>();
                for (KeySchemaElement key : idx1.keySchema()) {
                    keyMap1.put(key.keyType(), key);
                }
                for (KeySchemaElement key : idx2.keySchema()) {
                    keyMap2.put(key.keyType(), key);
                }

                for (KeyType keyType : keyMap1.keySet()) {
                    Assert.assertTrue(
                            "Key type " + keyType + " not found in local index " + indexName,
                            keyMap2.containsKey(keyType));
                    Assert.assertEquals(
                            "Attribute name mismatch for " + keyType + " key in local index "
                                    + indexName, keyMap1.get(keyType).attributeName(),
                            keyMap2.get(keyType).attributeName());
                }
            }
        }

        Thread.sleep(1000);

        // creation date
        Assert.assertTrue("DDB table should have been created before Phoenix table",
                tableDescription1.creationDateTime()
                        .isBefore(tableDescription2.creationDateTime()));
        Assert.assertTrue(
                tableDescription2.creationDateTime()
                        .isBefore(new Date(System.currentTimeMillis()).toInstant()));

        Assert.assertNull(tableDescription2.tableArn());

        // stream spec
        StreamSpecification streamSpec1 = tableDescription1.streamSpecification();
        StreamSpecification streamSpec2 = tableDescription2.streamSpecification();
        Assert.assertEquals(streamSpec1, streamSpec2);
    }

    /**
     * Verify CDC metadata in TableDescription returned from Phoenix client.
     * CDC index, SCHEMA_VERSION column in PTable and timestamp.
     */
    public static void assertCDCMetadata(PhoenixConnection pconn, TableDescription td,
                                         String streamType)
            throws SQLException, ParseException {
        String tableName = td.tableName();
        Assert.assertTrue(td.latestStreamArn().startsWith(
                "arn:aws:dynamodb:us-west-2:000000000000:table/"));
        Assert.assertTrue(td.latestStreamArn().contains("/stream/"));
        Assert.assertTrue(td.latestStreamArn().contains(tableName));

        PTable dataTable = pconn.getTable(PhoenixUtils.getFullTableName(tableName, false));
        Assert.assertEquals(streamType, dataTable.getSchemaVersion());
        boolean cdcIndexPresent = false;
        PTable cdcIndex = null;
        for (PTable index : dataTable.getIndexes()) {
            if (CDCUtil.isCDCIndex(PhoenixUtils.getTableNameFromFullName(index.getName().getString(), false))) {
                cdcIndexPresent = true;
                cdcIndex = index;
            }
        }
        Assert.assertTrue(cdcIndexPresent);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = df.parse(td.latestStreamLabel());
        Assert.assertEquals(String.valueOf(cdcIndex.getTimeStamp()), String.valueOf(date.getTime()));
        Assert.assertTrue(td.latestStreamArn().endsWith("/stream/" + td.latestStreamLabel()));
    }

    private static CreateTableRequest.Builder addGlobalIndexToRequest(CreateTableRequest.Builder request, String indexName,
                                                List<KeySchemaElement> keySchemaElements, List<GlobalSecondaryIndex> indexList) {
        List<GlobalSecondaryIndex> indexes = new ArrayList<>(indexList);
        indexes.add(GlobalSecondaryIndex.builder().indexName(indexName)
                        .keySchema(keySchemaElements)
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(50L).writeCapacityUnits(50L).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build());
        return request.globalSecondaryIndexes(indexes);
    }

    private static CreateTableRequest.Builder addLocalIndexToRequest(CreateTableRequest.Builder request, String indexName,
                                               List<KeySchemaElement> keySchemaElements, List<LocalSecondaryIndex> indexList) {
        List<LocalSecondaryIndex> indexes = new ArrayList<>(indexList);
        indexes.add(LocalSecondaryIndex.builder().indexName(indexName)
                .keySchema(keySchemaElements)
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build());
        return request.localSecondaryIndexes(indexes);
    }
}
