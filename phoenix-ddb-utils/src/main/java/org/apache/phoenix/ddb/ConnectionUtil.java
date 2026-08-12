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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Phoenix client connections.
 * Loads connection properties from a configuration file at startup
 * and uses them to create connections.
 */
public class ConnectionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtil.class);
    private static final String CLIENT_CONNECTION_CONFIG_FILE =
            "phoenix-client-connection.properties";

    private static final Properties PROPS;

    static {
        try {
            PROPS = loadConfiguration();
        } catch (IOException e) {
            LOGGER.error("Failed to load Phoenix connection configuration", e);
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Returns a mutable copy of the connection properties.
     * Use this when you need to add or modify properties for a specific connection.
     */
    public static Properties getMutableProps() {
        return new Properties(PROPS);
    }

    /**
     * Get a Connection with the given url and loaded config properties.
     */
    public static Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url, PROPS);
    }

    private static Properties loadConfiguration() throws IOException {
        Properties props = new Properties();
        try (InputStream is = ConnectionUtil.class.getClassLoader()
                .getResourceAsStream(CLIENT_CONNECTION_CONFIG_FILE)) {
            if (is != null) {
                props.load(is);
                LOGGER.info("Loaded Phoenix connection configuration from {}: {}",
                        CLIENT_CONNECTION_CONFIG_FILE, props);
            } else {
                throw new IOException(
                        "Configuration file not found: " + CLIENT_CONNECTION_CONFIG_FILE);
            }
        }
        return props;
    }
}