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

package org.apache.phoenix.ddb.service.utils;

import java.util.Base64;

/**
 * Helper class to hold the start and end keys for a segment.
 * If both keys are null, it means the segment is empty.
 */
public class ScanSegmentInfo {

    private final String tableName;
    private final int totalSegments;
    private final int segmentNumber;
    private final byte[] startKey;
    private final byte[] endKey;

    public ScanSegmentInfo(String tableName, int totalSegments, int segmentNumber, byte[] startKey,
            byte[] endKey) {
        this.tableName = tableName;
        this.totalSegments = totalSegments;
        this.segmentNumber = segmentNumber;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public boolean isEmptySegment() {
        return startKey == null && endKey == null;
    }

    public String toShortString() {
        return String.format("Segment %d/%d", segmentNumber, totalSegments);
    }

    @Override
    public String toString() {
        if (isEmptySegment()) {
            return "ScanSegmentInfo [tableName=" + tableName + ", totalSegments=" + totalSegments
                    + ", segmentNumber=" + segmentNumber + ", startKey=null"
                    + ", endKey=null" + "]";
        }
        return "ScanSegmentInfo [tableName=" + tableName + ", totalSegments=" + totalSegments
                + ", segmentNumber=" + segmentNumber + ", startKey=" +
                Base64.getEncoder().encodeToString(startKey) + ", endKey="
                + Base64.getEncoder().encodeToString(endKey) + "]";
    }
}