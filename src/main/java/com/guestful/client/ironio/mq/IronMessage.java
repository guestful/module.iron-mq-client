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
import javax.json.JsonObject;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;

/**
 * date 2014-06-04
 *
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronMessage {

    private final IronQueue queue;
    private final String id;
    private final JsonObject body;
    private final int timeout;
    private boolean deleted = false;
    private boolean released = false;

    IronMessage(IronQueue queue, String id, JsonObject body, int timeout) {
        this.queue = queue;
        this.id = id;
        this.body = body;
        this.timeout = timeout;
    }

    /**
     * Timeout when the message was created
     */
    public int getTimeout() {
        return timeout;
    }

    public IronQueue getQueue() {
        return queue;
    }

    public String getId() {
        return id;
    }

    public JsonObject getBody() {
        return body;
    }

    public boolean isReleased() {
        return released;
    }

    public boolean isDeleted() {
        return deleted;
    }

    /**
     * Touching a reserved message extends its timeout to the duration specified when the message was created.
     * See {@link #getTimeout()}
     * Default is 60 seconds.
     */
    public void touch() {
        if (isDeleted()) {
            throw new IronException("Message " + getId() + " is deleted");
        }
        Response response = getQueue().request(HttpMethod.POST, "messages/" + getId() + "/touch");
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            throw new IronClientException(response);
        }
    }

    /**
     * Releasing a reserved message unreserves the message and puts it back on the queue as if the message had timed out.
     */
    public void release() {
        release(getQueue().getProject().getSettings().getMessageDelay(), TimeUnit.SECONDS);
    }

    public void release(long delay, TimeUnit unit) {
        if (isReleased()) throw new IronException("Message " + getId() + " is released");
        if (isDeleted()) throw new IronException("Message " + getId() + " is deleted");
        IronSettings settings = getQueue().getProject().getSettings().copy().setMessageDelay(delay, unit);
        JsonObject body = Json.createObjectBuilder()
            .add("delay", settings.getMessageDelay())
            .build();
        Response response = getQueue().request(HttpMethod.POST, "messages/" + getId() + "/release", body);
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            throw new IronClientException(response);
        }
        released = true;
    }

    public void delete() {
        if (isDeleted()) return;
        Response response = getQueue().request(HttpMethod.DELETE, "messages/" + getId());
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL && response.getStatus() != 404) {
            throw new IronClientException(response);
        }
        deleted = true;
    }

    @Override
    public String toString() {
        return getId() + " " + getBody();
    }

}
