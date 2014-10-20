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
import javax.json.JsonObjectBuilder;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronSubscriber {

    private final String url;
    private final Map<String, String> headers = new LinkedHashMap<>();

    public IronSubscriber(String url) {
        this.url = url;
    }

    public IronSubscriber header(String name, String val) {
        this.headers.put(name, val);
        return this;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    JsonObject toJson() {
        JsonObjectBuilder h = Json.createObjectBuilder();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            h.add(entry.getKey(), entry.getValue());
        }
        return Json.createObjectBuilder()
            .add("url", getUrl())
            .add("headers", h.build())
            .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IronSubscriber that = (IronSubscriber) o;
        return url.equals(that.url);
    }

    @Override
    public int hashCode() {
        return url.hashCode();
    }

    @Override
    public String toString() {
        return getUrl();
    }
}
