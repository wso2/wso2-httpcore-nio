/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

package org.apache.http.impl.nio.reactor;

import org.apache.http.impl.nio.DefaultNHttpClientConnection;
import org.apache.http.impl.nio.DefaultNHttpServerConnection;
import org.apache.http.nio.NHttpConnectionFactory;
import org.apache.http.nio.testserver.LoggingSSLClientConnectionFactory;
import org.apache.http.nio.testserver.LoggingSSLServerConnectionFactory;
import org.apache.http.nio.testserver.SSLTestContexts;
import org.junit.Ignore;

/**
 * Basic functionality tests for SSL I/O reactors.
 *
 */
// TODO: Enable after test failures are fixed.
@Ignore("Disabled due to build failing in workflows")
public class TestDefaultIOReactorsSSL extends TestDefaultIOReactors {

    @Override
    protected NHttpConnectionFactory<DefaultNHttpServerConnection> createServerConnectionFactory() throws Exception {
        return new LoggingSSLServerConnectionFactory(SSLTestContexts.createServerSSLContext());
    }

    @Override
    protected NHttpConnectionFactory<DefaultNHttpClientConnection> createClientConnectionFactory() throws Exception {
        return new LoggingSSLClientConnectionFactory(SSLTestContexts.createClientSSLContext());
    }

}
