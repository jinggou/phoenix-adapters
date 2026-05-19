package org.apache.phoenix.ddb.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;

public class DescribeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStreamService.class);
    private static final int MAX_LIMIT = 100;

    private static final String DESCRIBE_STREAM_QUERY
            = "SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME FROM "
            + SYSTEM_CDC_STREAM_NAME + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' ";

    private static final String PARENT_PARTITION_START_TIMES_QUERY
            = "SELECT PARTITION_ID, PARTITION_START_TIME FROM "
            + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' AND PARTITION_ID IN (%s)";

    public static Map<String, Object> describeStream(Map<String, Object> request,
        String connectionUrl) {
        String streamArnInput = (String) request.get(ApiMetadata.STREAM_ARN);
        String streamName = DdbAdapterCdcUtils.normalizeStreamName(streamArnInput);
        String streamArn = DdbAdapterCdcUtils.isStreamArn(streamArnInput) ?
            streamArnInput : DdbAdapterCdcUtils.toStreamArn(streamName);
        String exclusiveStartShardId = (String) request.get(ApiMetadata.EXCLUSIVE_START_SHARD_ID);
        Integer limit = (Integer) request.getOrDefault(ApiMetadata.LIMIT, MAX_LIMIT);
        String tableName = DdbAdapterCdcUtils.getTableNameFromStreamName(streamName);
        Map<String, Object> streamDesc;
        try (Connection conn = ConnectionUtil.getConnection(connectionUrl)) {
            streamDesc = getStreamDescriptionObject(conn, tableName, streamName, streamArn);
            String streamStatus = DdbAdapterCdcUtils.getStreamStatus(conn, tableName, streamName);
            streamDesc.put(ApiMetadata.STREAM_STATUS, streamStatus);
            // query partitions only if stream is ENABLED
            if (CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue().equals(streamStatus)) {
                StringBuilder sb = new StringBuilder(String.format(DESCRIBE_STREAM_QUERY, tableName, streamName));
                if (!StringUtils.isEmpty(exclusiveStartShardId)) {
                    sb.append(" AND PARTITION_ID > '");
                    sb.append(DdbAdapterCdcUtils.partitionIdFromShardId(exclusiveStartShardId));
                    sb.append("'");
                }
                sb.append(" LIMIT ");
                sb.append(limit);
                LOGGER.debug("Describe Stream Query: {}", sb);

                List<Map<String, Object>> rawShards = new ArrayList<>();
                Map<String, Long> partitionStartTimes = new HashMap<>();
                Set<String> parentIdsNeeded = new HashSet<>();
                ResultSet rs = conn.createStatement().executeQuery(sb.toString());
                int count = 0;
                while (rs.next()) {
                    count++;
                    Map<String, Object> raw = new HashMap<>();
                    String partitionId = rs.getString(1);
                    String parentPartitionId = rs.getString(2);
                    long partitionStartTime = rs.getLong(3);
                    long partitionEndTime = rs.getLong(4);
                    raw.put("partitionId", partitionId);
                    raw.put("parentPartitionId", parentPartitionId);
                    raw.put("partitionStartTime", partitionStartTime);
                    raw.put("partitionEndTime", partitionEndTime);
                    rawShards.add(raw);
                    partitionStartTimes.put(partitionId, partitionStartTime);
                    if (parentPartitionId != null) {
                        parentIdsNeeded.add(parentPartitionId);
                    }
                }
                // Parents already in the current page don't need a second query.
                parentIdsNeeded.removeAll(partitionStartTimes.keySet());

                if (!parentIdsNeeded.isEmpty()) {
                    String inClause = parentIdsNeeded.stream()
                            .map(id -> "'" + id + "'")
                            .collect(Collectors.joining(","));
                    String parentQuery = String.format(PARENT_PARTITION_START_TIMES_QUERY,
                            tableName, streamName, inClause);
                    LOGGER.debug("Parent Partition Start Times Query: {}", parentQuery);
                    ResultSet prs = conn.createStatement().executeQuery(parentQuery);
                    while (prs.next()) {
                        partitionStartTimes.put(prs.getString(1), prs.getLong(2));
                    }
                }

                List<Map<String, Object>> shards = new ArrayList<>();
                String lastEvaluatedShardId = null;
                for (Map<String, Object> raw : rawShards) {
                    Map<String, Object> shard = buildShardMetadata(raw, partitionStartTimes);
                    shards.add(shard);
                    lastEvaluatedShardId = (String) shard.get(ApiMetadata.SHARD_ID);
                }
                streamDesc.put(ApiMetadata.SHARDS, shards);
                if (count == limit) {
                    streamDesc.put(ApiMetadata.LAST_EVALUATED_SHARD_ID, lastEvaluatedShardId);
                }
            }
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        Map<String, Object> result = new HashMap<>();
        result.put(ApiMetadata.STREAM_DESCRIPTION, streamDesc);
        return result;
    }

    /**
     * Return a StreamDescription object for the given tableName and streamName.
     * Populate all attributes except the list of the shards.
     */
    private static Map<String, Object> getStreamDescriptionObject(Connection conn, String tableName,
        String streamName, String streamArn) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(tableName);
        Map<String, Object> streamDesc = new HashMap<>();
        streamDesc.put(ApiMetadata.STREAM_ARN, streamArn);
        streamDesc.put(ApiMetadata.TABLE_NAME, PhoenixUtils.getTableNameFromFullName(tableName, false));
        long creationTS = DdbAdapterCdcUtils.getCDCIndexTimestampFromStreamName(streamName);
        streamDesc.put(ApiMetadata.STREAM_LABEL, DdbAdapterCdcUtils.getStreamLabel(streamName));
        streamDesc.put(ApiMetadata.STREAM_VIEW_TYPE, table.getSchemaVersion());
        streamDesc.put(ApiMetadata.CREATION_REQUEST_DATE_TIME,
                BigDecimal.valueOf(creationTS).movePointLeft(3));
        streamDesc.put(ApiMetadata.KEY_SCHEMA, DdbAdapterCdcUtils.getKeySchemaForRest(table));
        return streamDesc;
    }

    /**
     * Build a Shard response object from a buffered partition row and a precomputed map
     * of {@code partitionId -> partitionStartTime} (including any parents pulled from the
     * batch parent lookup).
     */
    private static Map<String, Object> buildShardMetadata(Map<String, Object> raw,
        Map<String, Long> partitionStartTimes) {
        String partitionId = (String) raw.get("partitionId");
        String parentPartitionId = (String) raw.get("parentPartitionId");
        long partitionStartTime = (Long) raw.get("partitionStartTime");
        long partitionEndTime = (Long) raw.get("partitionEndTime");

        Map<String, Object> shard = new HashMap<>();
        shard.put(ApiMetadata.SHARD_ID,
            DdbAdapterCdcUtils.toShardId(partitionStartTime, partitionId));
        if (parentPartitionId != null) {
            Long parentStartTime = partitionStartTimes.get(parentPartitionId);
            if (parentStartTime != null) {
                shard.put(ApiMetadata.PARENT_SHARD_ID,
                    DdbAdapterCdcUtils.toShardId(parentStartTime, parentPartitionId));
            } else {
                LOGGER.info("Parent partition {} for partition {} is no longer present in "
                    + "SYSTEM.CDC_STREAM (likely TTLed); omitting ParentShardId "
                    + "from the response.", parentPartitionId, partitionId);
            }
        }
        Map<String, Object> seqNumRange = new HashMap<>();
        seqNumRange.put(ApiMetadata.STARTING_SEQUENCE_NUMBER,
            DdbAdapterCdcUtils.getSequenceNumber(partitionStartTime, 0));
        if (partitionEndTime > 0) {
            seqNumRange.put(ApiMetadata.ENDING_SEQUENCE_NUMBER,
                DdbAdapterCdcUtils.getSequenceNumber(partitionEndTime,
                    MAX_NUM_CHANGES_AT_TIMESTAMP - 1));
        }
        shard.put(ApiMetadata.SEQUENCE_NUMBER_RANGE, seqNumRange);
        return shard;
    }
}
