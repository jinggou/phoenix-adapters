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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test for PHOENIX-7900: phoenix-adapters should throw status code 400
 * when a required placeholder value is not present in the request
 */
public class QueryMissingExpressionAttributeValueIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryMissingExpressionAttributeValueIT.class);

    private static DynamoDbClient phoenixDBClientV2;
    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (restServer != null) {
            restServer.stop();
        }
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) {
                utility.shutdownMiniCluster();
            }
            ServerMetadataCacheTestImpl.resetCache();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }

    @Test(timeout = 120000)
    public void testQueryWithMissingPartitionKeyValue() throws Exception {
        // Create table
        final String tableName = "TestMissingAttrValue";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk",
                        ScalarAttributeType.S, "sk", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);

        // Put an item
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.builder().s("test-pk").build());
        item.put("sk", AttributeValue.builder().s("test-sk").build());
        item.put("data", AttributeValue.builder().s("some-data").build());
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        phoenixDBClientV2.putItem(putItemRequest);

        // Query with KeyConditionExpression that references :v0, but don't provide :v0 in ExpressionAttributeValues
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("pk = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        qr.expressionAttributeNames(exprAttrNames);
        // Intentionally missing ExpressionAttributeValues
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        // :v0 is missing!
        qr.expressionAttributeValues(exprAttrVal);

        boolean exceptionThrown = false;
        try {
            phoenixDBClientV2.query(qr.build());
        } catch (DynamoDbException e) {
            exceptionThrown = true;
            // Should get 400 ValidationException, not 500 InternalServerError
            Assert.assertEquals("Expected status code 400 but got " + e.statusCode() + ": " + e.getMessage(),
                    400, e.statusCode());
            Assert.assertTrue("Expected ValidationException in: " + e.getMessage(),
                    e.getMessage().contains("ValidationException"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception type: " + e.getClass().getName() + ": " + e.getMessage());
        }
        Assert.assertTrue("Expected an exception to be thrown", exceptionThrown);
    }

    @Test(timeout = 120000)
    public void testQueryWithMissingSortKeyValue() throws Exception {
        // Create table
        final String tableName = "TestMissingSortKeyValue";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk",
                        ScalarAttributeType.S, "sk", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);

        // Put an item
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.builder().s("test-pk").build());
        item.put("sk", AttributeValue.builder().n("10").build());
        item.put("data", AttributeValue.builder().s("some-data").build());
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        phoenixDBClientV2.putItem(putItemRequest);

        // Query with KeyConditionExpression pk = :v0 AND sk < :v1
        // Provide :v0 but not :v1
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("pk = :v0 AND sk < :v1");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("test-pk").build());
        // :v1 is missing!
        qr.expressionAttributeValues(exprAttrVal);

        boolean exceptionThrown = false;
        try {
            phoenixDBClientV2.query(qr.build());
        } catch (DynamoDbException e) {
            exceptionThrown = true;
            // Should get 400 ValidationException, not 500 InternalServerError
            Assert.assertEquals("Expected status code 400 but got " + e.statusCode() + ": " + e.getMessage(),
                    400, e.statusCode());
            Assert.assertTrue("Expected ValidationException in: " + e.getMessage(),
                    e.getMessage().contains("ValidationException"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception type: " + e.getClass().getName() + ": " + e.getMessage());
        }
        Assert.assertTrue("Expected an exception to be thrown", exceptionThrown);
    }

    @Test(timeout = 120000)
    public void testQueryWithMissingBetweenValue() throws Exception {
        // Create table
        final String tableName = "TestMissingBetweenValue";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk",
                        ScalarAttributeType.S, "sk", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);

        // Put an item
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.builder().s("test-pk").build());
        item.put("sk", AttributeValue.builder().n("10").build());
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        phoenixDBClientV2.putItem(putItemRequest);

        // Query with BETWEEN but missing second value
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("pk = :v0 AND sk BETWEEN :v1 AND :v2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("test-pk").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("5").build());
        // :v2 is missing!
        qr.expressionAttributeValues(exprAttrVal);

        boolean exceptionThrown = false;
        try {
            phoenixDBClientV2.query(qr.build());
        } catch (DynamoDbException e) {
            exceptionThrown = true;
            // Should get 400 ValidationException, not 500 InternalServerError
            Assert.assertEquals("Expected status code 400 but got " + e.statusCode() + ": " + e.getMessage(),
                    400, e.statusCode());
            Assert.assertTrue("Expected ValidationException in: " + e.getMessage(),
                    e.getMessage().contains("ValidationException"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception type: " + e.getClass().getName() + ": " + e.getMessage());
        }
        Assert.assertTrue("Expected an exception to be thrown", exceptionThrown);
    }
}
