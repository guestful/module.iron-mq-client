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
import javax.ws.rs.core.Response;
import java.util.Collection;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronClientException extends IronException {

    private final int statusCode;

    public IronClientException(int statusCode, String message) {
        super(statusCode + " " + message);
        this.statusCode = statusCode;
    }

    public IronClientException(Response response) {
        super(extractMessage(response));
        this.statusCode = response.getStatus();
    }

    public IronClientException(Collection<JsonObject> messages, Response response) {
        super(extractMessage(messages, response));
        this.statusCode = response.getStatus();
    }

    public int getStatusCode() {
        return statusCode;
    }

    private static String extractMessage(Response response) {
        JsonObject body = response.readEntity(JsonObject.class);
        return response.getStatus() + " " + response.getStatusInfo().getReasonPhrase() + (body == null ? "" : ": " + String.valueOf(body.getString("msg")));
    }

    private static String extractMessage(Collection<JsonObject> messages, Response response) {
        JsonObject body = response.readEntity(JsonObject.class);
        return response.getStatus() + " " + response.getStatusInfo().getReasonPhrase() + (body == null ? "" : ": " + String.valueOf(body.getString("msg")) + " Messages: " + messages);
    }

}
