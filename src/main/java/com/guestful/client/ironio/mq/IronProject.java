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

import javax.json.*;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronProject {

    private static char[] RFC_3986_Reserved_Characters = "!*'();:@&=+$,/?#[]".toCharArray();

    private final IronClient client;
    private final String id;
    private final String token;
    private final IronSettings settings = new IronSettings();

    IronProject(IronClient client, String id, String token) {
        this.client = client;
        this.id = id;
        this.token = token;
    }

    public IronSettings getSettings() {
        return settings;
    }

    public IronClient getClient() {
        return client;
    }

    public String getId() {
        return id;
    }

    public String getToken() {
        return token;
    }

    @Override
    public String toString() {
        return getId();
    }

    public Collection<IronQueue> getQueues() {
        Response response = request(getSettings(), HttpMethod.GET, "queues");
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            throw new IronClientException(response);
        }
        return response.readEntity(JsonArray.class)
            .stream()
            .map(jsonValue -> getQueue(((JsonObject) jsonValue).getString("name")))
            .collect(Collectors.toList());
    }

    public IronQueue getQueue(String name) {
        ensureValidQueueName(name);
        return new IronQueue(this, name);
    }

    public IronQueue newPullQueue(String name) {
        return newQueue(name, IronQueueType.PULL, Collections.emptyList(), getSettings());
    }

    public IronQueue newUnicastQueue(String name) {
        return newQueue(name, IronQueueType.UNICAST, Collections.emptyList(), getSettings());
    }

    public IronQueue newUnicastQueue(String name, Collection<IronSubscriber> subscribers) {
        return newQueue(name, IronQueueType.UNICAST, subscribers, getSettings());
    }

    public IronQueue newUnicastQueue(String name, Collection<IronSubscriber> subscribers, IronSettings settings) {
        return newQueue(name, IronQueueType.UNICAST, subscribers, settings);
    }

    public IronQueue newMulticastQueue(String name) {
        return newQueue(name, IronQueueType.MULTICAST, Collections.emptyList(), getSettings());
    }

    public IronQueue newMulticastQueue(String name, Collection<IronSubscriber> subscribers) {
        return newQueue(name, IronQueueType.MULTICAST, subscribers, getSettings());
    }

    public IronQueue newQueue(String name, IronQueueType type, Collection<IronSubscriber> subscribers, IronSettings settings) {
        ensureValidQueueName(name);
        JsonObjectBuilder body = Json.createObjectBuilder()
            .add("push_type", type.name().toLowerCase());
        if (type == IronQueueType.UNICAST || type == IronQueueType.MULTICAST) {
            JsonArrayBuilder subs = Json.createArrayBuilder();
            for (IronSubscriber subscriber : subscribers) {
                subs.add(subscriber.toJson());
            }
            body
                .add("retries", settings.getPushRetries())
                .add("retries_delay", settings.getPushRetryDelay())
                .add("subscribers", subs.build());
            if (settings.getErrorQueuename() != null) {
                body.add("error_queue", settings.getErrorQueuename());
            }
        }
        Response response = request(settings, HttpMethod.POST, "queues/" + name, body.build());
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            throw new IronClientException(response);
        }
        return new IronQueue(this, name);
    }

    Response request(IronSettings settings, String method, String path) {
        if (!path.startsWith("/")) path = "/" + path;
        MultivaluedMap<String, Object> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("oauth", getToken());
        return getClient().request(settings, method, "projects/" + getId() + path, queryParams);
    }

    Response request(IronSettings settings, String method, String path, JsonObject body) {
        if (!path.startsWith("/")) path = "/" + path;
        MultivaluedMap<String, Object> queryParams = new MultivaluedHashMap<>();
        queryParams.putSingle("oauth", getToken());
        return getClient().request(settings, method, "projects/" + getId() + path, body, queryParams);
    }

    Response request(IronSettings settings, String method, String path, MultivaluedMap<String, Object> queryParams) {
        if (!path.startsWith("/")) path = "/" + path;
        queryParams.putSingle("oauth", getToken());
        return getClient().request(settings, method, "projects/" + getId() + path, queryParams);
    }

    private static void ensureValidQueueName(String name) {
        for (char c : RFC_3986_Reserved_Characters) {
            if (name.indexOf(c) != -1) {
                throw new IllegalArgumentException("Queue name cannot contains any of these characters: " + new String(RFC_3986_Reserved_Characters));
            }
        }
    }

}
