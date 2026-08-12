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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication filter that validates requests using AWS-style Access Key ID.
 * <p>
 * This filter supports multiple ways to provide access keys:
 * 1. Authorization header with AWS4-HMAC-SHA256 format (extracts access id only)
 * 2. Authorization header with AccessKeyId format  
 * 3. X-Access-Key-Id header
 * </p>
 * <p>
 * Usage:
 * 1. Implement the CredentialStore interface for your storage mechanism
 * 2. Register this filter with your CredentialStore implementation
 * 3. Configure clients to send access keys in one of the supported formats
 * </p>
 */
public class AccessKeyAuthFilter implements Filter {
    
    private static final Logger LOG = LoggerFactory.getLogger(AccessKeyAuthFilter.class);
    
    private static final Pattern ACCESS_KEY_PATTERN = Pattern.compile(
        "(?:AccessKeyId|Credential)=([^/\\s,]+)"
    );
    
    private CredentialStore credentialStore;

    public AccessKeyAuthFilter() {
    }

    public AccessKeyAuthFilter(CredentialStore credentialStore) {
        this.credentialStore = credentialStore;
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        if (httpRequest.getRequestURI().startsWith("/jmx")) {
            chain.doFilter(request, response);
            return;
        }
        
        try {
            ValidationResult result = validateAccessKey(httpRequest);
            
            if (result.isValid()) {
                httpRequest.setAttribute("userName", result.getUserName());
                httpRequest.setAttribute("accessKeyId", result.getAccessKeyId());
                
                chain.doFilter(request, response);
            } else {
                LOG.warn("Authentication failed: {}", result.getErrorMessage());
                
                httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
                httpResponse.setContentType("application/json");
                httpResponse.getWriter().write(
                    "{\"error\":\"Forbidden\",\"message\":\"" + result.getErrorMessage() + "\"}"
                );
            }
        } catch (Exception e) {
            LOG.error("Error while authenticating", e);
            httpResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            httpResponse.setContentType("application/json");
            httpResponse.getWriter().write(
                "{\"error\":\"Internal Server Error\",\"message\":\"Authentication error\"}"
            );
        }
    }
    
    /**
     * Validates the access key from the request.
     * Supports multiple authentication methods:
     * 1. Authorization header with AWS4-HMAC-SHA256 format (extracts access key only)
     * 2. Authorization header with AccessKeyId format
     * 3. X-Access-Key-Id header
     */
    private ValidationResult validateAccessKey(HttpServletRequest request) {
        String accessKeyId = extractAccessKeyId(request);
        if (accessKeyId == null || accessKeyId.trim().isEmpty()) {
            return ValidationResult.failure("Missing access key ID");
        }
        UserCredentials userCreds = credentialStore.getCredentials(accessKeyId);
        if (userCreds == null) {
            return ValidationResult.failure("Invalid access key: " + accessKeyId);
        }
        return ValidationResult.success(userCreds.getUserName(), accessKeyId);
    }
    
    /**
     * Extracts access key ID from various sources in the request.
     */
    private String extractAccessKeyId(HttpServletRequest request) {
        // 1. Try Authorization header
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null) {
            Matcher matcher = ACCESS_KEY_PATTERN.matcher(authHeader);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        
        // 2. Try X-Access-Key-Id header
        String accessKeyHeader = request.getHeader("X-Access-Key-Id");
        if (accessKeyHeader != null && !accessKeyHeader.trim().isEmpty()) {
            return accessKeyHeader.trim();
        }
        return null;
    }
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("Access Key Authentication Filter initialized");
    }
    
    @Override
    public void destroy() {
        LOG.info("Access Key Authentication Filter destroyed");
    }
    
    /**
     * Validation result class.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final String userName;
        private final String accessKeyId;
        private final String errorMessage;

        private ValidationResult(boolean valid, String userName, String accessKeyId,
                String errorMessage) {
            this.valid = valid;
            this.userName = userName;
            this.accessKeyId = accessKeyId;
            this.errorMessage = errorMessage;
        }
        
        public static ValidationResult success(String userName, String accessKeyId) {
            return new ValidationResult(true, userName, accessKeyId, null);
        }
        
        public static ValidationResult failure(String errorMessage) {
            return new ValidationResult(false, null, null, errorMessage);
        }

        public boolean isValid() {
            return valid;
        }

        public String getUserName() {
            return userName;
        }

        public String getAccessKeyId() {
            return accessKeyId;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }
}