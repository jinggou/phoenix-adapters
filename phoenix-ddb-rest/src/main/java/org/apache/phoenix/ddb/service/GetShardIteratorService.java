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

package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils;
import org.apache.phoenix.ddb.utils.PhoenixShardIterator;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GetShardIteratorService {

    public static Map<String, Object> getShardIterator(Map<String, Object> request,
                                                       String connectionUrl) {
        Map<String, Object> result = new HashMap<>();
        try (Connection conn = ConnectionUtil.getConnection(connectionUrl)) {
            String streamArnInput = (String) request.get(ApiMetadata.STREAM_ARN);
            String streamName = DdbAdapterCdcUtils.normalizeStreamName(streamArnInput);
            String partitionId = DdbAdapterCdcUtils.partitionIdFromShardId(
                (String) request.get(ApiMetadata.SHARD_ID));
            String seqNum = (String) request.get(ApiMetadata.SEQUENCE_NUMBER);
            String shardIterType = (String) request.get(ApiMetadata.SHARD_ITERATOR_TYPE);
            String tableName = DdbAdapterCdcUtils.getTableNameFromStreamName(streamName);
            String startSeqNum =
                getStartingSequenceNumber(conn, tableName, streamName, partitionId, seqNum,
                    shardIterType);
            String streamType = DdbAdapterCdcUtils.getStreamType(conn, tableName);
            String streamArn = DdbAdapterCdcUtils.isStreamArn(streamArnInput)
                ? streamArnInput : DdbAdapterCdcUtils.toStreamArn(streamName);
            PhoenixShardIterator pIter = new PhoenixShardIterator(streamArn, streamName,
                streamType, partitionId, startSeqNum);
            result.put(ApiMetadata.SHARD_ITERATOR, pIter.toString());
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        return result;
    }

    private static String getStartingSequenceNumber(Connection conn, String tableName,
                                                    String streamName, String partitionId,
                                                    String seqNum, String type)
            throws SQLException {
        String startSeqNum;
        switch (type) {
            case "AT_SEQUENCE_NUMBER" :
                startSeqNum =
                    String.format("%021d", DdbAdapterCdcUtils.parseSequenceNumber(seqNum));
                break;
            case "AFTER_SEQUENCE_NUMBER":
                startSeqNum =
                    String.format("%021d", DdbAdapterCdcUtils.parseSequenceNumber(seqNum) + 1);
                break;
            case "LATEST":
                // new records only i.e. use current time.
                startSeqNum =
                    DdbAdapterCdcUtils.getSequenceNumber(EnvironmentEdgeManager.currentTimeMillis(),
                        0);
                break;
            case "TRIM_HORIZON":
                // Oldest available sequence number in the shard, we will use shard's start sequence number
                long partitionStartTime = DdbAdapterCdcUtils.getPartitionStartTime(
                        conn, tableName, streamName, partitionId);
                startSeqNum = DdbAdapterCdcUtils.getSequenceNumber(partitionStartTime, 0);
                break;
        default:
                throw new ValidationException("Invalid shard iterator type: " + type);
        }
        return startSeqNum;
    }
}
