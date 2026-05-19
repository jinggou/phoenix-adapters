package org.apache.phoenix.ddb.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.OFFSET_LENGTH;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_DELIM;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_NUM_PARTS;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_VERSION;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SI_FIELD_PARTITION_ID;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SI_FIELD_SEQ_NUM;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SI_FIELD_STREAM_TYPE;

/**
 * Class to represent a shard iterator for Phoenix CDC queries.
 * The format:
 * <pre>
 *   &lt;streamArn&gt;|&lt;version&gt;|&lt;base64(JSON state)&gt;
 * </pre>
 */
public class PhoenixShardIterator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern DELIM_PATTERN =
        Pattern.compile(Pattern.quote(SHARD_ITERATOR_DELIM));
    private static final TypeReference<Map<String, String>> STATE_TYPE_REF =
        new TypeReference<Map<String, String>>() {
        };

    private final String streamArn;
    private final String tableName;
    private final String cdcObject;

    private final String streamType;
    private final String partitionId;
    private String seqNum;
    private long timestamp;
    private int offset;

    /**
     * Build a fresh iterator from server-side state. Used by
     * {@code GetShardIteratorService} when emitting a brand-new iterator.
     *
     * @param streamArn   stream ARN
     * @param streamName  internal Phoenix stream name
     * @param streamType  stream view type (NEW_IMAGE, OLD_IMAGE, KEYS_ONLY,
     *                    NEW_AND_OLD_IMAGES)
     * @param partitionId region encoded partition id
     * @param seqNum      resume sequence number
     */
    public PhoenixShardIterator(String streamArn, String streamName, String streamType,
        String partitionId, String seqNum) {
        this.streamArn = streamArn;
        this.tableName = DdbAdapterCdcUtils.getTableNameFromStreamName(streamName);
        this.cdcObject = DdbAdapterCdcUtils.getCDCObjectNameFromStreamName(streamName);
        this.streamType = streamType;
        this.partitionId = partitionId;
        this.seqNum = seqNum;
        setTimestampAndOffset();
    }

    public PhoenixShardIterator(String shardIterator) {
        if (shardIterator == null || shardIterator.isEmpty()) {
            throw new IllegalArgumentException("ShardIterator is required");
        }
        String[] parts = DELIM_PATTERN.split(shardIterator, -1);
        if (parts.length != SHARD_ITERATOR_NUM_PARTS) {
            throw new IllegalArgumentException(
                "ShardIterator must be of the form <streamArn>|<version>|<base64-state>: "
                    + shardIterator);
        }
        String parsedStreamArn = parts[0];
        String version = parts[1];
        String base64State = parts[2];
        if (!SHARD_ITERATOR_VERSION.equals(version)) {
            throw new IllegalArgumentException(
                "Unsupported ShardIterator version: " + version + " (expected "
                    + SHARD_ITERATOR_VERSION + ")");
        }
        String internalStreamName = DdbAdapterCdcUtils.fromStreamArn(parsedStreamArn);
        this.streamArn = parsedStreamArn;
        this.tableName = DdbAdapterCdcUtils.getTableNameFromStreamName(internalStreamName);
        this.cdcObject = DdbAdapterCdcUtils.getCDCObjectNameFromStreamName(internalStreamName);

        Map<String, String> state = decodeState(base64State, shardIterator);
        this.streamType = state.get(SI_FIELD_STREAM_TYPE);
        this.partitionId = state.get(SI_FIELD_PARTITION_ID);
        this.seqNum = state.get(SI_FIELD_SEQ_NUM);
        if (this.streamType == null || this.partitionId == null || this.seqNum == null) {
            throw new IllegalArgumentException(
                "ShardIterator state missing required fields (" + SI_FIELD_STREAM_TYPE + ", "
                    + SI_FIELD_PARTITION_ID + ", " + SI_FIELD_SEQ_NUM + "): " + shardIterator);
        }
        setTimestampAndOffset();
    }

    public String getStreamArn() {
        return streamArn;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCdcObject() {
        return cdcObject;
    }

    public String getStreamType() {
        return streamType;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getSeqNum() {
        return seqNum;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getOffset() {
        return offset;
    }

    public void setNewSeqNum(long timestamp, int offset) {
        this.timestamp = timestamp;
        this.offset = offset;
        this.seqNum = DdbAdapterCdcUtils.getSequenceNumber(timestamp, offset);
    }

    @Override
    public String toString() {
        Map<String, String> state = new LinkedHashMap<>();
        state.put(SI_FIELD_STREAM_TYPE, streamType);
        state.put(SI_FIELD_PARTITION_ID, partitionId);
        state.put(SI_FIELD_SEQ_NUM, seqNum);
        byte[] jsonBytes;
        try {
            jsonBytes = OBJECT_MAPPER.writeValueAsBytes(state);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize ShardIterator state", e);
        }
        String base64State = Base64.getEncoder().encodeToString(jsonBytes);
        return streamArn + SHARD_ITERATOR_DELIM + SHARD_ITERATOR_VERSION + SHARD_ITERATOR_DELIM
            + base64State;
    }

    private static Map<String, String> decodeState(String base64State, String shardIterator) {
        byte[] stateBytes;
        try {
            stateBytes = Base64.getDecoder().decode(base64State);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "ShardIterator base64 state is not decodable: " + shardIterator, e);
        }
        try {
            return OBJECT_MAPPER.readValue(stateBytes, STATE_TYPE_REF);
        } catch (IOException e) {
            String decoded = new String(stateBytes, StandardCharsets.UTF_8);
            throw new IllegalArgumentException("ShardIterator state is not valid JSON: " + decoded,
                e);
        }
    }

    private void setTimestampAndOffset() {
        String timestampStr = this.seqNum.substring(0, this.seqNum.length() - OFFSET_LENGTH);
        String offsetStr = this.seqNum.substring(this.seqNum.length() - OFFSET_LENGTH);
        this.timestamp = Long.parseLong(timestampStr);
        this.offset = Integer.parseInt(offsetStr);
    }
}
