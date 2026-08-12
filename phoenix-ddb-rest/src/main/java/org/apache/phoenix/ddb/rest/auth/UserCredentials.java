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

package org.apache.phoenix.ddb.rest.auth;

/**
 * Class for the User credentials data.
 * <p>
 * This is a simple immutable data class that holds the essential
 * information needed for authentication:
 * - userName: Human-readable username or identifier
 * - accessKeyId: access key
 * - secretKey: Secret key
 * </p>
 */
public class UserCredentials {

    private final String userName;
    private final String accessKeyId;
    private final String secretKey;

    /**
     * Creates new user credentials.
     * 
     * @param userName Human-readable username or identifier.
     * @param accessKeyId Access key ID.
     * @param secretKey Secret key.
     */
    public UserCredentials(String userName, String accessKeyId, String secretKey) {
        this.userName = userName;
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
    }
    
    /**
     * @return The human-readable username or identifier.
     */
    public String getUserName() { 
        return userName; 
    }
    
    /**
     * @return The AWS-style access key ID.
     */
    public String getAccessKeyId() { 
        return accessKeyId; 
    }
    
    /**
     * @return The secret key.
     */
    public String getSecretKey() { 
        return secretKey; 
    }
}