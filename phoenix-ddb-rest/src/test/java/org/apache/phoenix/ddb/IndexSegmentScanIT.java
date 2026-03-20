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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Segment scan tests for indexes.
 * Verifies that segment scans on indexes produce the same results as a full index scan.
 */
@RunWith(Parameterized.class)
public class IndexSegmentScanIT extends BaseSegmentScanIT {

    private final String hashKeyName;
    private final ScalarAttributeType hashKeyType;
    private final String sortKeyName;
    private final ScalarAttributeType sortKeyType;
    private final String indexHashKeyName;
    private final ScalarAttributeType indexHashKeyType;
    private final String indexSortKeyName;
    private final ScalarAttributeType indexSortKeyType;
    private final int totalSegments;
    private final boolean useFilter;
    private final int filterNum;

    protected static final int TOTAL_ITEMS = 3000;
    protected static final int SPLIT_FREQUENCY = 600;

    public IndexSegmentScanIT(String hashKeyName, ScalarAttributeType hashKeyType,
            String sortKeyName, ScalarAttributeType sortKeyType,
            String indexHashKeyName, ScalarAttributeType indexHashKeyType,
            String indexSortKeyName, ScalarAttributeType indexSortKeyType,
            boolean useFilter) {
        this.hashKeyName = hashKeyName;
        this.hashKeyType = hashKeyType;
        this.sortKeyName = sortKeyName;
        this.sortKeyType = sortKeyType;
        this.indexHashKeyName = indexHashKeyName;
        this.indexHashKeyType = indexHashKeyType;
        this.indexSortKeyName = indexSortKeyName;
        this.indexSortKeyType = indexSortKeyType;
        Random random = new Random();
        this.totalSegments = random.nextInt(6) + 2;
        this.useFilter = useFilter;
        this.filterNum = useFilter ? random.nextInt(15) + 1 : 0;
    }

    @Parameterized.Parameters(name = "TblH_{1}_TblS_{3}_IdxH_{5}_IdxS_{7}_Filter_{8}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // HK-only table, IHK-only index
                {"pk", ScalarAttributeType.S, null, null,
                        "status", ScalarAttributeType.S, null, null, false},
                {"pk", ScalarAttributeType.N, null, null,
                        "category", ScalarAttributeType.S, null, null, true},

                // HK+SK table, IHK-only index
                {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N,
                        "status", ScalarAttributeType.S, null, null, true},
                {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.S,
                        "category", ScalarAttributeType.S, null, null, false},

                // HK-only table, IHK+ISK index
                {"pk", ScalarAttributeType.S, null, null,
                        "status", ScalarAttributeType.S, "priority", ScalarAttributeType.N, false},
                {"pk", ScalarAttributeType.N, null, null,
                        "category", ScalarAttributeType.S, "amount", ScalarAttributeType.N, true},

                // HK+SK table, IHK+ISK index
                {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N,
                        "status", ScalarAttributeType.S, "priority", ScalarAttributeType.N, true},
                {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.S,
                        "category", ScalarAttributeType.S, "amount", ScalarAttributeType.N, false},

                // Binary table keys with index
                {"pk", ScalarAttributeType.B, null, null,
                        "status", ScalarAttributeType.S, null, null, true},
                {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.B,
                        "category", ScalarAttributeType.S, "priority", ScalarAttributeType.N, false},
        });
    }

    // Split points that divide the index key space for String index hash keys.
    // Works for both "status" (active/cancelled/completed/failed/pending)
    // and "category" (books/clothing/electronics/food/sports/toys) values.
    private static final String[] INDEX_SPLIT_POINTS = {"c", "f", "p", "s"};

    @Test(timeout = 600000)
    public void testIndexSegmentScan() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_")
                + "_" + generateRandomString(5);
        final String indexName = "idx_" + generateRandomString(5);

        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName,
                indexHashKeyName, indexHashKeyType, indexSortKeyName, indexSortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        insertItemsAndSplitIndex(tableName, indexName, TOTAL_ITEMS, SPLIT_FREQUENCY, 200);

        Random random = new Random();

        // Full scan on index - both Phoenix and DDB
        List<Map<String, AttributeValue>> fullScanItemsPhoenix =
                performFullScanWithPagination(phoenixDBClientV2, tableName, indexName,
                        useFilter, filterNum, random.nextInt(150) + 1);
        List<Map<String, AttributeValue>> fullScanItemsDDB =
                performFullScanWithPagination(dynamoDbClient, tableName, indexName,
                        useFilter, filterNum, random.nextInt(200) + 1);

        // Segment scan on index
        List<Map<String, AttributeValue>> segmentScanItems = new ArrayList<>();
        for (int segment = 0; segment < totalSegments; segment++) {
            int scanLimit = random.nextInt(200) + 1;
            List<Map<String, AttributeValue>> segmentItems =
                    scanSingleSegmentWithPagination(tableName, indexName, segment, totalSegments,
                            scanLimit, false, useFilter, filterNum);
            segmentScanItems.addAll(segmentItems);
        }

        TestUtils.verifyItemsEqual(fullScanItemsDDB, fullScanItemsPhoenix, hashKeyName, sortKeyName);
        TestUtils.verifyItemsEqual(fullScanItemsPhoenix, segmentScanItems, hashKeyName, sortKeyName);
    }

    /**
     * Insert items and periodically split the index table (not the data table).
     */
    private void insertItemsAndSplitIndex(String tableName, String indexName,
            int totalItems, int splitFrequency, int sleepMillis) throws Exception {
        int splitCount = 0;
        List<WriteRequest> batch = new ArrayList<>();
        for (int i = 0; i < totalItems; i++) {
            Map<String, AttributeValue> item =
                    createTestItem(i, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
            batch.add(WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build()).build());
            boolean shouldFlush = batch.size() >= 25 || (i > 0 && (i + 1) % splitFrequency == 0)
                    || i == totalItems - 1;
            if (shouldFlush) {
                executeBatchWrite(tableName, batch);
                batch.clear();
            }
            if (i > 0 && i % splitFrequency == 0 && splitCount < INDEX_SPLIT_POINTS.length) {
                splitIndexTable(tableName, indexName, splitCount++);
                Thread.sleep(sleepMillis);
            }
        }
    }

    private void splitIndexTable(String tableName, String indexName, int splitIndex) {
        try {
            byte[] splitPoint = Bytes.toBytes(INDEX_SPLIT_POINTS[splitIndex]);
            String fullIndexTableName = PhoenixUtils.getFullTableName(
                    PhoenixUtils.getInternalIndexName(tableName, indexName), false);
            TestUtils.splitTable(testConnection, fullIndexTableName, splitPoint);
            LOGGER.info("Split index table {} at '{}'", indexName,
                    INDEX_SPLIT_POINTS[splitIndex]);
        } catch (Exception e) {
            LOGGER.warn("Failed to split index table: {}", e.getMessage());
        }
    }

    private String generateRandomString(int length) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }
}
