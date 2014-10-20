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

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronQueue {

    private static final Logger LOGGER = Logger.getLogger(IronQueue.class.getName());

    private final IronProject project;
    private final String name;

    IronQueue(IronProject project, String name) {
        this.project = project;
        this.name = name;
    }

    public IronProject getProject() {
        return project;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        Response response = getProject().request(getProject().getSettings(), HttpMethod.GET, "queues/" + getEncodedQueueName());
        switch (response.getStatus()) {
            case 404:
                return 0;
            case 200:
                JsonObject body = response.readEntity(JsonObject.class);
                return body.getJsonNumber("size").longValue();
            default:
                throw new IronClientException(response);
        }
    }

    public long getCount() {
        Response response = getProject().request(getProject().getSettings(), HttpMethod.GET, "queues/" + getEncodedQueueName());
        switch (response.getStatus()) {
            case 404:
                return 0;
            case 200:
                JsonObject body = response.readEntity(JsonObject.class);
                return body.getJsonNumber("total_messages").longValue();
            default:
                throw new IronClientException(response);
        }
    }

    public void offer(JsonObject message) {
        offer(Arrays.asList(message), getProject().getSettings());
    }

    public void offer(JsonObject message, long delay, TimeUnit unit) {
        offer(Arrays.asList(message), getProject().getSettings().copy().setMessageDelay(delay, unit));
    }

    public void offer(JsonObject... messages) {
        offer(Arrays.asList(messages), getProject().getSettings());
    }

    public void offer(Collection<JsonObject> messages, long delay, TimeUnit unit) {
        offer(messages, getProject().getSettings().copy().setMessageDelay(delay, unit));
    }

    public void offer(Collection<JsonObject> messages) {
        offer(messages, getProject().getSettings());
    }

    public void offer(JsonObject message, IronSettings settings) {
        offer(Arrays.asList(message), settings);
    }

    public void offer(Collection<JsonObject> messages, IronSettings settings) {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (JsonObject message : messages) {
            arrayBuilder.add(Json.createObjectBuilder()
                .add("body", message.toString())
                .add("timeout", settings.getMessageTimeout())
                .add("delay", settings.getMessageDelay())
                .add("expires_in", settings.getMessageExpiration())
                .build());
        }
        JsonObject body = Json.createObjectBuilder()
            .add("messages", arrayBuilder.build())
            .build();
        Response response = getProject().request(settings, HttpMethod.POST, "queues/" + getEncodedQueueName() + "/messages", body);
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            throw new IronClientException(messages, response);
        }
    }

    public boolean delete() {
        Response response = getProject().request(getProject().getSettings(), HttpMethod.DELETE, "queues/" + getEncodedQueueName());
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL && response.getStatus() != 404) {
            throw new IronClientException(response);
        }
        return response.getStatus() != 404;
    }

    @Override
    public String toString() {
        return getName();
    }

    Response request(String method, String path) {
        if (!path.startsWith("/")) path = "/" + path;
        return getProject().request(getProject().getSettings(), method, "queues/" + getEncodedQueueName() + path);
    }

    Response request(String method, String path, JsonObject body) {
        if (!path.startsWith("/")) path = "/" + path;
        return getProject().request(getProject().getSettings(), method, "queues/" + getEncodedQueueName() + path, body);
    }

    Response request(String method, String path, MultivaluedMap<String, Object> queryParams) {
        if (!path.startsWith("/")) path = "/" + path;
        return getProject().request(getProject().getSettings(), method, "queues/" + getEncodedQueueName() + path, queryParams);
    }

    String getEncodedQueueName() {
        try {
            return URLEncoder.encode(getName(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public IronPoller asyncPoll(Consumer<IronMessage> consumer) {
        return asyncPoll(Runnable::run, consumer);
    }

    public IronPoller asyncPoll(Executor executor, Consumer<IronMessage> consumer) {
        return asyncPoll(
            executor,
            consumer,
            (message, e) -> LOGGER.log(Level.SEVERE, "Error while processing message " + message.getId() + " from queue " + message.getQueue().getName() + " from project " + message.getQueue().getProject().getId() + ": " + e.getMessage() + "\nMessage: " + message, e));
    }

    public IronPoller asyncPoll(Executor executor, Consumer<IronMessage> consumer, BiConsumer<IronMessage, RuntimeException> onError) {
        AtomicReference<Runnable> canRun = new AtomicReference<>();
        canRun.set(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && canRun.get() != null) {
                    LOGGER.finest("Polling queue " + getName() + "...");
                    IronMessage message = poll(IronSettings.MAX_WAIT, TimeUnit.SECONDS);
                    if (message != null) {
                        try {
                            consumer.accept(message);
                        } catch (RuntimeException e) {
                            onError.accept(message, e);
                            continue;
                        }
                        LOGGER.finest("Removing message " + message.getId());
                        message.delete();
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in poller for from queue " + getName() + " from project " + getProject().getId() + ": " + e.getMessage(), e);
            } finally {
                // try to re-execute
                Runnable me = canRun.get();
                if (me != null) {
                    executor.execute(me);
                }
            }
        });
        executor.execute(canRun.get());
        return new IronPoller() {
            @Override
            public IronQueue getQueue() {
                return IronQueue.this;
            }

            @Override
            public void stop() {
                canRun.set(null);
            }
        };
    }

    public IronMessage poll() {
        return poll(getProject().getSettings());
    }

    public IronMessage poll(long wait, TimeUnit unit) {
        return poll(getProject().getSettings().copy().setPollWait(wait, unit));
    }

    public IronMessage poll(IronSettings settings) {
        LOGGER.finest("poll() wait=" + settings.getPollWait());
        MultivaluedMap<String, Object> qParams = new MultivaluedHashMap<>();
        qParams.putSingle("n", 1);
        qParams.putSingle("wait", settings.getPollWait());
        qParams.putSingle("timeout", settings.getMessageTimeout());
        qParams.putSingle("delete", settings.isPollDelete());
        Response response = getProject().request(settings, HttpMethod.GET, "queues/" + getEncodedQueueName() + "/messages", qParams);
        if (response.getStatus() == 404) return null;
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            throw new IronClientException(response);
        }
        if (response.hasEntity()) {
            JsonObject body = response.readEntity(JsonObject.class);
            JsonArray list = body.getJsonArray("messages");
            if (list == null || list.isEmpty()) return null;
            JsonObject message = list.getJsonObject(0);
            JsonObject content = Json.createReader(new StringReader(message.getString("body"))).readObject();
            return new IronMessage(
                this,
                message.getString("id"),
                content,
                message.getInt("timeout"));
        }
        return null;
    }


}
