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
 * Interface for credential storage and retrieval.
 * <p>
 * Implementers can use any storage mechanism they prefer:
 * - Database (Phoenix, MySQL, PostgreSQL etc.)
 * - File-based storage (properties files, JSON etc.)
 * - LDAP/Active Directory
 * - In-memory storage (for testing)
 * - External services (Vault, Secret Service etc.)
 * </p>
 */
public interface CredentialStore {
    
    /**
     * Retrieves user credentials by access key ID.
     * 
     * @param accessKeyId The AWS-style access key ID
     * @return UserCredentials if found and valid, null otherwise
     */
    UserCredentials getCredentials(String accessKeyId);
} 