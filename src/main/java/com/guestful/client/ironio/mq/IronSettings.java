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

import java.util.concurrent.TimeUnit;

/**
 * date 2014-06-04
 *
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronSettings {

    public static final int MIN_TIMEOUT = 30;
    public static final int MAX_TIMEOUT = 86_400;
    public static final int DEF_TIMEOUT = 60;

    public static final int MIN_DELAY = 0;
    public static final int MAX_DELAY = 604_800;
    public static final int DEF_DELAY = MIN_DELAY;

    public static final int MIN_EXPIRATION = 0;
    public static final int MAX_EXPIRATION = 2_592_000;
    public static final int DEF_EXPIRATION = MAX_DELAY;

    public static final int MIN_WAIT = 0;
    public static final int MAX_WAIT = 30;
    public static final int DEF_WAIT = MIN_WAIT;

    public static final int MIN_RETRY = 0;
    public static final int MAX_RETRY = 100;
    public static final int DEF_RETRY = 3;

    public static final int MIN_RETRY_DELAY = 3;
    public static final int MAX_RETRY_DELAY = MAX_TIMEOUT;
    public static final int DEF_RETRY_DELAY = 60;

    public static final int DEF_BACKOFF_RETRY = 5;
    public static final int MIN_BACKOFF_RETRY = 0;

    public static final int DEF_BACKOFF_INTERVAL = 10;
    public static final int MIN_BACKOFF_INTERVAL = 0;
    public static final int MAX_BACKOFF_INTERVAL = MAX_TIMEOUT;

    public static final float DEF_BACKOFF_FACTOR = 1.5f;
    public static final float MIN_BACKOFF_FACTOR = 1.0f;

    private int messageTimeout = DEF_TIMEOUT;
    private int messageDelay = DEF_DELAY;
    private int messageExpiration = DEF_EXPIRATION;
    private int pollWait = DEF_WAIT;
    private boolean pollDelete = false;
    private int pushRetries = DEF_RETRY;
    private int pushRetryDelay = DEF_RETRY_DELAY;
    private String errorQueuename;
    private int backoffRetries = DEF_BACKOFF_RETRY;
    private int backoffInterval = DEF_BACKOFF_INTERVAL;
    private float backoffFactor = DEF_BACKOFF_FACTOR;

    public float getBackoffFactor() {
        return backoffFactor;
    }

    public IronSettings setBackoffFactor(float backoffFactor) {
        if (backoffFactor < MIN_BACKOFF_FACTOR) throw new IllegalArgumentException();
        this.backoffFactor = backoffFactor;
        return this;
    }

    public int getBackoffInterval() {
        return backoffInterval;
    }

    public IronSettings setBackoffInterval(long backoffInterval, TimeUnit unit) {
        long t = unit.toSeconds(backoffInterval);
        if (t < MIN_BACKOFF_INTERVAL || t > MAX_BACKOFF_INTERVAL) throw new IllegalArgumentException();
        this.backoffInterval = Math.toIntExact(t);
        return this;
    }

    public int getBackoffRetries() {
        return backoffRetries;
    }

    public IronSettings setBackoffRetries(int backoffRetries) {
        if (backoffRetries < MIN_BACKOFF_RETRY) throw new IllegalArgumentException();
        this.backoffRetries = backoffRetries;
        return this;
    }

    public String getErrorQueuename() {
        return errorQueuename;
    }

    public IronSettings setErrorQueuename(String errorQueuename) {
        this.errorQueuename = errorQueuename;
        return this;
    }

    public boolean isPollDelete() {
        return pollDelete;
    }

    /**
     * true/false. This will delete the message on get. Be careful though, only use this if you are ok with losing a message if something goes wrong after you get it. Default is false.
     */
    public IronSettings setPollDelete(boolean pollDelete) {
        this.pollDelete = pollDelete;
        return this;
    }

    public int getPollWait() {
        return pollWait;
    }

    /**
     * Time in seconds to wait for a message to become available. This enables long polling. Default is 0 (does not wait), maximum is 30.
     */
    public IronSettings setPollWait(long wait, TimeUnit unit) {
        long t = unit.toSeconds(wait);
        if (t < MIN_WAIT || t > MAX_WAIT) throw new IllegalArgumentException();
        this.pollWait = Math.toIntExact(t);
        return this;
    }

    public int getMessageTimeout() {
        return messageTimeout;
    }

    /**
     * After timeout (in seconds), item will be placed back onto queue. You must delete the message from the queue to ensure it does not go back onto the queue. Default is 60 seconds. Minimum is 30 seconds, and maximum is 86,400 seconds (24 hours).
     */
    public IronSettings setMessageTimeout(long timeout, TimeUnit unit) {
        long t = unit.toSeconds(timeout);
        if (t < MIN_TIMEOUT || t > MAX_TIMEOUT) throw new IllegalArgumentException();
        this.messageTimeout = Math.toIntExact(t);
        return this;
    }

    public int getMessageDelay() {
        return messageDelay;
    }

    /**
     * The item will not be available on the queue until this many seconds have passed. Default is 0 seconds. Maximum is 604,800 seconds (7 days).
     */
    public IronSettings setMessageDelay(long delay, TimeUnit unit) {
        long t = unit.toSeconds(delay);
        if (t < MIN_DELAY || t > MAX_DELAY) throw new IllegalArgumentException();
        this.messageDelay = Math.toIntExact(t);
        return this;
    }

    public int getMessageExpiration() {
        return messageExpiration;
    }

    /**
     * How long in seconds to keep the item on the queue before it is deleted. Default is 604,800 seconds (7 days). Maximum is 2,592,000 seconds (30 days).
     */
    public IronSettings setMessageExpiration(long expiration, TimeUnit unit) {
        long t = unit.toSeconds(expiration);
        if (t < MIN_EXPIRATION || t > MAX_EXPIRATION) throw new IllegalArgumentException();
        this.messageExpiration = Math.toIntExact(t);
        return this;
    }

    public int getPushRetries() {
        return pushRetries;
    }

    /**
     * Number of times to retry. Default is 3. Maximum is 100.
     */
    public IronSettings setPushRetries(int pushRetries) {
        if (pushRetries < MIN_RETRY || pushRetries > MAX_RETRY) throw new IllegalArgumentException();
        this.pushRetries = pushRetries;
        return this;
    }

    public int getPushRetryDelay() {
        return pushRetryDelay;
    }

    /**
     * Time in seconds between retries. Default is 60. Minimum is 3 and maximum is 86400 seconds.
     */
    public IronSettings setPushRetryDelay(long pushRetryDelay, TimeUnit unit) {
        long t = unit.toSeconds(pushRetryDelay);
        if (t < MIN_RETRY_DELAY || t > MAX_RETRY_DELAY) throw new IllegalArgumentException();
        this.pushRetryDelay = Math.toIntExact(t);
        return this;
    }

    public IronSettings copy() {
        return new IronSettings()
            .setMessageTimeout(getMessageTimeout(), TimeUnit.SECONDS)
            .setMessageExpiration(getMessageExpiration(), TimeUnit.SECONDS)
            .setMessageDelay(getMessageDelay(), TimeUnit.SECONDS)
            .setPollWait(getPollWait(), TimeUnit.SECONDS)
            .setPollDelete(isPollDelete())
            .setPushRetries(getPushRetries())
            .setPushRetryDelay(getPushRetryDelay(), TimeUnit.SECONDS)
            .setErrorQueuename(getErrorQueuename())
            .setBackoffFactor(getBackoffFactor())
            .setBackoffInterval(getBackoffInterval(), TimeUnit.SECONDS)
            .setBackoffRetries(getBackoffRetries());
    }

}
