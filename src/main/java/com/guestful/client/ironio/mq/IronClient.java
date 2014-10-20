/**
 * Copyright (C) 2013 Guestful (info@guestful.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.guestful.client.ironio.mq;

import javax.json.JsonObject;
import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronClient {

    private static final Logger LOGGER = Logger.getLogger(IronClient.class.getName());

    private final Client client;
    private final WebTarget target;
    private boolean enabled = true;

    public IronClient() {
        this(ClientBuilder.newClient());
    }

    public IronClient(Client restClient) {
        this.client = restClient;
        this.target = buildWebTarget();
    }

    public Client getClient() {
        return client;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public IronProject getProject(String projectId, String token) {
        return new IronProject(this, projectId, token);
    }

    protected WebTarget buildWebTarget() {
        return getClient().target("http://mq-aws-us-east-1.iron.io/1");
    }

    Response request(IronSettings settings, String method, String path, MultivaluedMap<String, Object> query) {
        return request(settings, method, path, null, query);
    }

    Response request(IronSettings settings, String method, String path, JsonObject message, MultivaluedMap<String, Object> query) {
        if (!isEnabled()) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(method + " " + path + (message == null ? "" : " : " + message));
            }
            return Response.ok().build();
        }
        WebTarget webTarget = target.path(path);
        for (String param : query.keySet()) {
            webTarget = webTarget.queryParam(param, query.getFirst(param));
        }
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        if (message == null) {
            return new BackoffResponse(settings, () -> {
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest(method + " " + path);
                }
                return builder.method(method);
            }).get();
        } else {
            return new BackoffResponse(settings, () -> {
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest(method + " " + path + ": " + message);
                }
                return builder.method(method, Entity.json(message));
            }).get();
        }
    }

}
