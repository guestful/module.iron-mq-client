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

import javax.ws.rs.core.Response;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
class BackoffResponse implements Supplier<Response> {

    private static final Logger LOGGER = Logger.getLogger(BackoffResponse.class.getName());

    private final IronSettings settings;
    private final Supplier<Response> supplier;

    BackoffResponse(IronSettings settings, Supplier<Response> supplier) {
        this.settings = settings;
        this.supplier = supplier;
    }

    @Override
    public Response get() {
        // if no backoff, directly execute call
        if (settings.getBackoffInterval() == 0 || settings.getBackoffRetries() == 0) {
            return supplier.get();
        }
        // variables to hold consecutive execution states
        int retries = 0;
        long sleep = settings.getBackoffInterval() * 1000;
        Response response = null;
        RuntimeException err = null;
        while (response == null || retries <= settings.getBackoffRetries()) {
            try {
                // try make call
                if (retries > 0) {
                    LOGGER.finest("backoff() retry=" + retries + "/" + settings.getBackoffRetries());
                }
                response = supplier.get();
                if (response.getStatusInfo().getFamily() != Response.Status.Family.SERVER_ERROR) {
                    // in case of success, returns response
                    return response;
                } else {
                    // otherwise just log the status and reason
                    LOGGER.finest("backoff() " + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
                }
            } catch (RuntimeException e) {
                // capture processing errors if any
                LOGGER.log(Level.WARNING, "backoff() err: " + e.getMessage(), e);
                err = e;
            }
            // here we have an err or a status 500
            retries++;
            if (retries <= settings.getBackoffRetries()) {
                // if we can retry, sleep
                try {
                    LOGGER.finest("backoff() sleep=" + sleep);
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.finest("backoff() sleep interrupted");
                    throw new IronException(e.getMessage(), e);
                }
                // then prepare next call
                err = null;
                response = null;
                sleep = Math.round(sleep * settings.getBackoffFactor());
            }
        }
        LOGGER.finest("backoff() no retry left");
        // we cannot retry anymore
        if (err != null) {
            throw err;
        } else {
            throw new IllegalStateException("backoff() no retry left and no response!");
        }
    }

}
