/*
 * Copyright 2012-2023 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.exporter;

import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.TlsPolicy;

/**
 * Configuration data.
 */
public class Parameters {
	String host;
	int port;
	String user;
	String password;
	String namespace;
	String sets;
	AuthMode authMode;
	TlsPolicy tlsPolicy;
	boolean tlsEnabled;
	// TODO: Support proxy client
	boolean useProxyClient;

	protected Parameters(TlsPolicy policy, boolean tlsEnabled, String host, int port, String user, String password, AuthMode authMode) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.authMode = authMode;
		this.tlsPolicy = policy;
		this.tlsEnabled = tlsEnabled;
	}

	@Override
	public String toString() {
		return "Conn Parameters: host=" + host +
				" port=" + port +
				" user=" + user +
				" authmode=" + authMode +
				" tlsEnable=" + tlsEnabled;

	}
}
