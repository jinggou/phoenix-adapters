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

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.TimeZone;

import org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils;
import org.apache.phoenix.ddb.utils.PhoenixShardIterator;
import org.junit.Assert;
import org.junit.Test;

public class DdbAdapterCdcUtilsTest {

    private static final long CDC_INDEX_TS = 1700000000000L;
    private static final String TABLE_NAME = "MY_TABLE";
    private static final String INTERNAL_TABLE = "DDB." + TABLE_NAME;

    private static String formatUtc(long ts) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        df.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return df.format(new Date(ts));
    }

    private static String internalStreamName(long ts) {
        return "phoenix/cdc/stream/" + INTERNAL_TABLE + "/CDC_" + TABLE_NAME + "/" + ts + "/"
            + formatUtc(ts);
    }

    @Test
    public void testToStreamArn_emitsAwsShape() {
        String arn = DdbAdapterCdcUtils.toStreamArn(internalStreamName(CDC_INDEX_TS));
        Assert.assertEquals(
            "arn:aws:dynamodb:us-west-2:000000000000:table/" + TABLE_NAME + "/stream/" + formatUtc(
                CDC_INDEX_TS), arn);
    }

    @Test
    public void testToStreamArn_stripsInternalDdbPrefix() {
        String arn = DdbAdapterCdcUtils.toStreamArn(internalStreamName(CDC_INDEX_TS));
        Assert.assertFalse(arn.contains("DDB." + TABLE_NAME));
        Assert.assertTrue(arn.contains(":table/" + TABLE_NAME + "/stream/"));
    }

    @Test
    public void testFromStreamArn_addsInternalDdbPrefix() {
        String arn =
            "arn:aws:dynamodb:us-west-2:000000000000:table/" + TABLE_NAME + "/stream/" + formatUtc(
                CDC_INDEX_TS);
        Assert.assertEquals(internalStreamName(CDC_INDEX_TS),
            DdbAdapterCdcUtils.fromStreamArn(arn));
    }

    @Test
    public void testFromStreamArn_idempotentOnLegacyArnWithDdbPrefix() {
        String legacyArn =
            "arn:aws:dynamodb:us-west-2:000000000000:table/" + INTERNAL_TABLE + "/stream/"
                + formatUtc(CDC_INDEX_TS);
        Assert.assertEquals(internalStreamName(CDC_INDEX_TS),
            DdbAdapterCdcUtils.fromStreamArn(legacyArn));
    }

    @Test
    public void testNormalizeThenToStreamArn_canonicalizesDdbPrefixedArn() {
        String prefixedArn =
            "arn:aws:dynamodb:us-west-2:000000000000:table/" + INTERNAL_TABLE + "/stream/"
                + formatUtc(CDC_INDEX_TS);
        String canonicalArn =
            "arn:aws:dynamodb:us-west-2:000000000000:table/" + TABLE_NAME + "/stream/" + formatUtc(
                CDC_INDEX_TS);
        String normalized = DdbAdapterCdcUtils.normalizeStreamName(prefixedArn);
        Assert.assertEquals(canonicalArn, DdbAdapterCdcUtils.toStreamArn(normalized));
    }

    @Test
    public void testFromStreamArn_reconstructsInternalName() {
        String internal = internalStreamName(CDC_INDEX_TS);
        String arn = DdbAdapterCdcUtils.toStreamArn(internal);
        Assert.assertEquals(internal, DdbAdapterCdcUtils.fromStreamArn(arn));
    }

    @Test
    public void testRoundTrip_byteIdentical() {
        for (long ts : new long[] {0L, 1L, 1000L, 1700000000000L, 1777440690689L, 4102444800000L}) {
            String internal = internalStreamName(ts);
            Assert.assertEquals("ts=" + ts, internal,
                DdbAdapterCdcUtils.fromStreamArn(DdbAdapterCdcUtils.toStreamArn(internal)));
        }
    }

    @Test
    public void testIsStreamArn() {
        Assert.assertTrue(DdbAdapterCdcUtils.isStreamArn(
            "arn:aws:dynamodb:us-west-2:000000000000:table/T/stream/2024-01-15T10:30:00.000"));
        Assert.assertFalse(DdbAdapterCdcUtils.isStreamArn(internalStreamName(CDC_INDEX_TS)));
        Assert.assertFalse(DdbAdapterCdcUtils.isStreamArn(null));
        Assert.assertFalse(DdbAdapterCdcUtils.isStreamArn(""));
    }

    @Test
    public void testNormalizeStreamName_acceptsBothFormats() {
        String internal = internalStreamName(CDC_INDEX_TS);
        String arn = DdbAdapterCdcUtils.toStreamArn(internal);
        Assert.assertEquals(internal, DdbAdapterCdcUtils.normalizeStreamName(internal));
        Assert.assertEquals(internal, DdbAdapterCdcUtils.normalizeStreamName(arn));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStreamArn_rejectsNonArn() {
        DdbAdapterCdcUtils.fromStreamArn(internalStreamName(CDC_INDEX_TS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStreamArn_rejectsMissingStreamSegment() {
        DdbAdapterCdcUtils.fromStreamArn(
            "arn:aws:dynamodb:us-west-2:000000000000:table/T-without-stream-segment");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStreamArn_rejectsMalformedLabel() {
        DdbAdapterCdcUtils.fromStreamArn(
            "arn:aws:dynamodb:us-west-2:000000000000:table/T/stream/not-an-iso-timestamp");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStreamArn_rejectsImpossibleDayInMonth() {
        DdbAdapterCdcUtils.fromStreamArn(
            "arn:aws:dynamodb:us-west-2:000000000000:table/T/stream/2024-02-30T00:00:00.000");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStreamArn_rejectsFeb29InNonLeapYear() {
        DdbAdapterCdcUtils.fromStreamArn(
            "arn:aws:dynamodb:us-west-2:000000000000:table/T/stream/2023-02-29T00:00:00.000");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStreamArn_rejectsImpossibleHour() {
        DdbAdapterCdcUtils.fromStreamArn(
            "arn:aws:dynamodb:us-west-2:000000000000:table/T/stream/2024-01-15T25:00:00.000");
    }

    @Test
    public void testToStreamArn_preservesTableName() {
        String arn = DdbAdapterCdcUtils.toStreamArn(internalStreamName(CDC_INDEX_TS));
        Assert.assertTrue(arn.contains(":table/" + TABLE_NAME + "/stream/"));
    }

    @Test
    public void testGetStreamLabel_unchangedAfterRoundTrip() {
        String internal = internalStreamName(CDC_INDEX_TS);
        String arn = DdbAdapterCdcUtils.toStreamArn(internal);
        Assert.assertEquals(DdbAdapterCdcUtils.getStreamLabel(internal),
            DdbAdapterCdcUtils.getStreamLabel(DdbAdapterCdcUtils.fromStreamArn(arn)));
    }

    private static final String PARTITION_HEX = "1a2b3c4d5e6f7890abcdef0123456789";
    private static final long PARTITION_START_MS = 1700000000000L;

    @Test
    public void testToShardId_emitsAwsShape() {
        Assert.assertEquals("shardId-" + PARTITION_START_MS + "-" + PARTITION_HEX,
            DdbAdapterCdcUtils.toShardId(PARTITION_START_MS, PARTITION_HEX));
    }

    @Test
    public void testToShardId_lengthWithinAwsSpec() {
        String shardId = DdbAdapterCdcUtils.toShardId(PARTITION_START_MS, PARTITION_HEX);
        Assert.assertTrue("len=" + shardId.length(),
            shardId.length() >= 28 && shardId.length() <= 65);
    }

    @Test
    public void testIsShardId_detectsBothFormats() {
        Assert.assertTrue(
            DdbAdapterCdcUtils.isShardId("shardId-" + PARTITION_START_MS + "-" + PARTITION_HEX));
        Assert.assertFalse(DdbAdapterCdcUtils.isShardId(PARTITION_HEX));
        Assert.assertFalse(DdbAdapterCdcUtils.isShardId(null));
        Assert.assertFalse(DdbAdapterCdcUtils.isShardId(""));
    }

    @Test
    public void testPartitionIdFromShardId_extractsBareHexFromNewShape() {
        String shardId = DdbAdapterCdcUtils.toShardId(PARTITION_START_MS, PARTITION_HEX);
        Assert.assertEquals(PARTITION_HEX, DdbAdapterCdcUtils.partitionIdFromShardId(shardId));
    }

    @Test
    public void testPartitionIdFromShardId_passesThroughLegacyHex() {
        Assert.assertEquals(PARTITION_HEX,
            DdbAdapterCdcUtils.partitionIdFromShardId(PARTITION_HEX));
    }

    @Test
    public void testPartitionIdFromShardId_idempotent() {
        String shardId = DdbAdapterCdcUtils.toShardId(PARTITION_START_MS, PARTITION_HEX);
        String once = DdbAdapterCdcUtils.partitionIdFromShardId(shardId);
        Assert.assertEquals(once, DdbAdapterCdcUtils.partitionIdFromShardId(once));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionIdFromShardId_rejectsNull() {
        DdbAdapterCdcUtils.partitionIdFromShardId(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionIdFromShardId_rejectsEmpty() {
        DdbAdapterCdcUtils.partitionIdFromShardId("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionIdFromShardId_rejectsMissingHexSegment() {
        DdbAdapterCdcUtils.partitionIdFromShardId("shardId-" + PARTITION_START_MS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionIdFromShardId_rejectsTrailingDashWithoutHex() {
        DdbAdapterCdcUtils.partitionIdFromShardId("shardId-" + PARTITION_START_MS + "-");
    }

    private static final long SEQ_TS = 1738372050000L;
    private static final int SEQ_OFFSET = 420;
    private static final long SEQ_NUMERIC = 173837205000000420L;
    private static final String SEQ_18 = "173837205000000420";
    private static final String SEQ_21 = "000173837205000000420";

    @Test
    public void testGetSequenceNumber_emits21DigitZeroPadded() {
        String seq = DdbAdapterCdcUtils.getSequenceNumber(SEQ_TS, SEQ_OFFSET);
        Assert.assertEquals("len=" + seq.length(), 21, seq.length());
        Assert.assertEquals(SEQ_21, seq);
    }

    @Test
    public void testGetSequenceNumber_numericValueUnchangedFromLegacyConcat() {
        String legacyConcat =
            SEQ_TS + String.format("%0" + DdbAdapterCdcUtils.OFFSET_LENGTH + "d", SEQ_OFFSET);
        String paddedNew = DdbAdapterCdcUtils.getSequenceNumber(SEQ_TS, SEQ_OFFSET);
        Assert.assertEquals(Long.parseLong(legacyConcat), Long.parseLong(paddedNew));
    }

    @Test
    public void testParseSequenceNumber_acceptsBothFormatsIdentically() {
        Assert.assertEquals(SEQ_NUMERIC, DdbAdapterCdcUtils.parseSequenceNumber(SEQ_18));
        Assert.assertEquals(SEQ_NUMERIC, DdbAdapterCdcUtils.parseSequenceNumber(SEQ_21));
        Assert.assertEquals(SEQ_NUMERIC, DdbAdapterCdcUtils.parseSequenceNumber(
            DdbAdapterCdcUtils.getSequenceNumber(SEQ_TS, SEQ_OFFSET)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSequenceNumber_rejectsNull() {
        DdbAdapterCdcUtils.parseSequenceNumber(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSequenceNumber_rejectsEmpty() {
        DdbAdapterCdcUtils.parseSequenceNumber("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSequenceNumber_rejectsNonNumeric() {
        DdbAdapterCdcUtils.parseSequenceNumber("abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSequenceNumber_rejectsNegative() {
        DdbAdapterCdcUtils.parseSequenceNumber("-1");
    }

    @Test
    public void testGetEventId_stableAcrossSequenceNumberFormatChange() {
        String idLegacy = DdbAdapterCdcUtils.getEventId(TABLE_NAME, PARTITION_HEX, SEQ_18);
        String idNew = DdbAdapterCdcUtils.getEventId(TABLE_NAME, PARTITION_HEX, SEQ_21);
        Assert.assertEquals(idLegacy, idNew);
        Assert.assertEquals(32, idLegacy.length());
        Assert.assertTrue(idLegacy.matches("[0-9a-f]{32}"));
    }

    @Test
    public void testGetEventId_matchesPreWiCRawHashOnLegacySeqNum() {
        String expected;
        try {
            String input = TABLE_NAME + "|" + PARTITION_HEX + "|" + SEQ_18;
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            expected = String.format("%032x", new java.math.BigInteger(1, digest));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(expected,
            DdbAdapterCdcUtils.getEventId(TABLE_NAME, PARTITION_HEX, SEQ_18));
        Assert.assertEquals(expected,
            DdbAdapterCdcUtils.getEventId(TABLE_NAME, PARTITION_HEX, SEQ_21));
    }

    private static final String SHARD_ITER_STREAM_TYPE = "NEW_AND_OLD_IMAGES";

    private static String canonicalStreamArn() {
        return DdbAdapterCdcUtils.toStreamArn(internalStreamName(CDC_INDEX_TS));
    }

    private static PhoenixShardIterator newIterator() {
        return new PhoenixShardIterator(canonicalStreamArn(), internalStreamName(CDC_INDEX_TS),
            SHARD_ITER_STREAM_TYPE, PARTITION_HEX, SEQ_21);
    }

    @Test
    public void testShardIterator_toString_emitsAwsShape() {
        String token = newIterator().toString();
        Assert.assertTrue("must start with canonical streamArn: " + token,
            token.startsWith(canonicalStreamArn() + "|"));
        String[] parts = token.split("\\|", -1);
        Assert.assertEquals("expected 3 outer parts: " + token, 3, parts.length);
        Assert.assertEquals("version sentinel must be \"1\": " + token, "1", parts[1]);
        String json = new String(Base64.getDecoder().decode(parts[2]), StandardCharsets.UTF_8);
        Assert.assertTrue(json.contains("\"streamType\":\"" + SHARD_ITER_STREAM_TYPE + "\""));
        Assert.assertTrue(json.contains("\"partitionId\":\"" + PARTITION_HEX + "\""));
        Assert.assertTrue(json.contains("\"seqNum\":\"" + SEQ_21 + "\""));
    }

    @Test
    public void testShardIterator_roundTrip_recoversAllFields() {
        PhoenixShardIterator original = newIterator();
        PhoenixShardIterator parsed = new PhoenixShardIterator(original.toString());
        Assert.assertEquals(canonicalStreamArn(), parsed.getStreamArn());
        Assert.assertEquals(SHARD_ITER_STREAM_TYPE, parsed.getStreamType());
        Assert.assertEquals(PARTITION_HEX, parsed.getPartitionId());
        Assert.assertEquals(SEQ_21, parsed.getSeqNum());
        Assert.assertEquals(original.getTimestamp(), parsed.getTimestamp());
        Assert.assertEquals(original.getOffset(), parsed.getOffset());
    }

    @Test
    public void testShardIterator_roundTrip_derivesTableAndCdcObjectFromStreamArn() {
        PhoenixShardIterator parsed = new PhoenixShardIterator(newIterator().toString());
        Assert.assertEquals(INTERNAL_TABLE, parsed.getTableName());
        Assert.assertEquals("CDC_" + TABLE_NAME, parsed.getCdcObject());
    }

    @Test
    public void testShardIterator_roundTrip_byteIdenticalToString() {
        String first = newIterator().toString();
        String second = new PhoenixShardIterator(first).toString();
        Assert.assertEquals(first, second);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsNull() {
        new PhoenixShardIterator((String) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsEmpty() {
        new PhoenixShardIterator("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsMalformedOuter() {
        new PhoenixShardIterator(canonicalStreamArn() + "|1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsUnknownVersion() {
        String validInnerBase64 = newIterator().toString().split("\\|", -1)[2];
        new PhoenixShardIterator(canonicalStreamArn() + "|9|" + validInnerBase64);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsCorruptBase64() {
        new PhoenixShardIterator(canonicalStreamArn() + "|1|!!!not-base64!!!");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsMalformedJson() {
        String notJson = Base64.getEncoder().withoutPadding()
            .encodeToString("not-json".getBytes(StandardCharsets.UTF_8));
        new PhoenixShardIterator(canonicalStreamArn() + "|1|" + notJson);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsMissingStateField() {
        String partialState = Base64.getEncoder().withoutPadding()
            .encodeToString("{\"streamType\":\"NEW_IMAGE\"}".getBytes(StandardCharsets.UTF_8));
        new PhoenixShardIterator(canonicalStreamArn() + "|1|" + partialState);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardIterator_parse_rejectsLegacySlashFormat() {
        new PhoenixShardIterator(
            "shardIterator/" + INTERNAL_TABLE + "/CDC_" + TABLE_NAME + "/NEW_IMAGE/" + PARTITION_HEX
                + "/" + SEQ_21);
    }

    @Test
    public void testShardIterator_setNewSeqNum_preservesAllOtherFieldsThroughRoundTrip() {
        PhoenixShardIterator pIter = newIterator();
        long newTs = SEQ_TS + 1000;
        int newOffset = 5;
        pIter.setNewSeqNum(newTs, newOffset);
        Assert.assertEquals(DdbAdapterCdcUtils.getSequenceNumber(newTs, newOffset),
            pIter.getSeqNum());
        PhoenixShardIterator reparsed = new PhoenixShardIterator(pIter.toString());
        Assert.assertEquals(canonicalStreamArn(), reparsed.getStreamArn());
        Assert.assertEquals(SHARD_ITER_STREAM_TYPE, reparsed.getStreamType());
        Assert.assertEquals(PARTITION_HEX, reparsed.getPartitionId());
        Assert.assertEquals(INTERNAL_TABLE, reparsed.getTableName());
        Assert.assertEquals("CDC_" + TABLE_NAME, reparsed.getCdcObject());
        Assert.assertEquals(DdbAdapterCdcUtils.getSequenceNumber(newTs, newOffset),
            reparsed.getSeqNum());
        Assert.assertEquals(newTs, reparsed.getTimestamp());
        Assert.assertEquals(newOffset, reparsed.getOffset());
    }

    @Test
    public void testShardIterator_lengthWithinAwsSpec_realisticInputs() {
        String token = newIterator().toString();
        Assert.assertTrue("token must fit AWS [1, 2048] spec, was len=" + token.length(),
            token.length() >= 1 && token.length() <= 2048);
    }

    @Test
    public void testShardIterator_lengthWithinAwsSpec_longTableName() {
        StringBuilder longBare = new StringBuilder("T");
        while (longBare.length() < 200) {
            longBare.append("X");
        }
        String longBareStr = longBare.toString();
        String longStreamArn =
            "arn:aws:dynamodb:us-west-2:000000000000:table/" + longBareStr + "/stream/" + formatUtc(
                CDC_INDEX_TS);
        String longInternalStreamName =
            "phoenix/cdc/stream/DDB." + longBareStr + "/CDC_" + longBareStr + "/" + CDC_INDEX_TS
                + "/" + formatUtc(CDC_INDEX_TS);
        PhoenixShardIterator big =
            new PhoenixShardIterator(longStreamArn, longInternalStreamName, SHARD_ITER_STREAM_TYPE,
                PARTITION_HEX, SEQ_21);
        String token = big.toString();
        Assert.assertTrue("token must fit AWS [1, 2048] spec, was len=" + token.length(),
            token.length() <= 2048);
        PhoenixShardIterator reparsed = new PhoenixShardIterator(token);
        Assert.assertEquals(longStreamArn, reparsed.getStreamArn());
        Assert.assertEquals("DDB." + longBare, reparsed.getTableName());
    }
}
