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

package org.apache.phoenix.ddb.utils;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.RawBsonDocument;

import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;

import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_TTL_DELETE_EVENT_TYPE;

/**
 * Class which represents a Phoenix CDC Stream record.
 */
public class PhoenixStreamRecord {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String eventType;
    private Map<String, Object> preImage;
    private Map<String, Object> postImage;
    private Map<String, Object> keys;

    public PhoenixStreamRecord(String cdcJson, List<PColumn> pkCols) throws JsonProcessingException {
        Map<String, Object> map = OBJECT_MAPPER.readValue(cdcJson, Map.class);
        this.eventType = (String) map.get(CDC_EVENT_TYPE);
        this.preImage = BsonDocumentToMap.getFullItem(getBsonDocFromCdcMap(map, QueryConstants.CDC_PRE_IMAGE));
        this.postImage = BsonDocumentToMap.getFullItem(getBsonDocFromCdcMap(map, QueryConstants.CDC_POST_IMAGE));
        populateKeys(pkCols);
    }

    public String getEventType() {
        return eventType;
    }

    public Map<String, Object> getPreImage() {
        return preImage;
    }

    public Map<String, Object> getPostImage() {
        return postImage;
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

    public boolean hasPreImage() {
        return preImage != null && !preImage.isEmpty();
    }

    public boolean hasPostImage() {
        return postImage != null && !postImage.isEmpty();
    }

    public boolean isTTLDeleteEvent() {
        return CDC_TTL_DELETE_EVENT_TYPE.equals(eventType);
    }

    private void populateKeys(List<PColumn> pkCols) {
        Map<String, Object> imageToUse = hasPreImage() ? getPreImage() : getPostImage();
        keys = new HashMap<>();
        for (PColumn pkCol : pkCols) {
            String pk = pkCol.getName().toString();
            keys.put(pk, imageToUse.get(pk));
        }
    }

    private static RawBsonDocument getBsonDocFromCdcMap(Map<String, Object> map, String key) {
        return Optional.ofNullable(map.get(key))
                .map(m -> (Map<String, Object>) m)
                .map(m -> m.get("COL"))
                .map(String.class::cast)
                .map(Base64.getDecoder()::decode)
                .map(bytes -> new RawBsonDocument(bytes, 0, bytes.length))
                .orElse(null);
    }
}
