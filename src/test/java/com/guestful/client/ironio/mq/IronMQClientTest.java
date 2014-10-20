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

import org.glassfish.jersey.jsonp.JsonProcessingFeature;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
@RunWith(JUnit4.class)
public class IronMQClientTest {

    static {
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        LoggerFactory.getILoggerFactory();
    }

    @Test
    public void test_poll() throws IronClientException, InterruptedException {

        JsonObject data = Json.createObjectBuilder().add("mykey", "myvalue").build();
        System.out.println(data);

        Client restClient = ClientBuilder.newBuilder().build();
        restClient.register(JsonProcessingFeature.class);

        IronClient ironClient = new IronClient(restClient) {
            @Override
            protected WebTarget buildWebTarget() {
                return super.buildWebTarget();
                //return getClient().target("http://127.0.0.1:4444/1");
            }
        };

        IronProject project = ironClient.getProject(System.getenv("IRON_MQ_PROJECT_ID"), System.getenv("IRON_MQ_TOKEN"));
        project.getSettings()
            .setMessageDelay(2, TimeUnit.SECONDS)
            .setMessageExpiration(4, TimeUnit.HOURS)
            .setMessageTimeout(30, TimeUnit.SECONDS);

        IronQueue queue = project.getQueue("tests.unit-test-" + LocalDateTime.now().toString().replace(":", ""));

        queue.delete();
        assertEquals(0, queue.getSize());
        assertEquals(0, queue.getCount());

        queue.offer(data);
        assertEquals(1, queue.getSize());
        assertEquals(1, queue.getCount());

        // delay
        IronMessage message = queue.poll();
        assertNull(message);

        message = queue.poll(3, TimeUnit.SECONDS);
        assertNotNull(message);
        assertNotNull(message.getId());
        assertEquals(data, message.getBody());
        assertEquals(project.getSettings().getMessageTimeout(), message.getTimeout());
        assertEquals(1, queue.getSize());
        assertEquals(1, queue.getCount());

        // message reserved
        assertNull(queue.poll());

        // release
        message.release(0, TimeUnit.SECONDS);

        // then poll again
        IronMessage message2 = queue.poll();
        assertNotNull(message2);
        assertEquals(message.getId(), message2.getId());

        message.delete();
        assertEquals(0, queue.getSize());
        assertEquals(1, queue.getCount());

        try {
            message.touch();
            fail();
        } catch (Exception e) {
            assertEquals(IronException.class, e.getClass());
        }

        try {
            message.release();
            fail();
        } catch (Exception e) {
            assertEquals(IronException.class, e.getClass());
        }

        // has no effect
        message2.touch();
        message2.release();

        // ok to delete already deleted message
        message2.delete();

        // async

        ExecutorService executorService = Executors.newCachedThreadPool();

        CountDownLatch latch = new CountDownLatch(7);

        IronPoller poller = queue.asyncPoll(executorService, message1 -> {
            System.out.println(message1);
            latch.countDown();
        });

        queue.offer(data, 0, TimeUnit.SECONDS);
        queue.offer(data, 1, TimeUnit.SECONDS);
        queue.offer(data, 2, TimeUnit.SECONDS);
        queue.offer(data, 3, TimeUnit.SECONDS);
        queue.offer(Arrays.asList(data, data, data), 4, TimeUnit.SECONDS);

        latch.await();
        poller.stop();
        queue.delete();

    }

    @Test
    public void test_push() throws IronClientException, InterruptedException {
        Client restClient = ClientBuilder.newBuilder().build();
        restClient.register(JsonProcessingFeature.class);

        IronClient ironClient = new IronClient(restClient) {
            @Override
            protected WebTarget buildWebTarget() {
                return super.buildWebTarget();
                //return restClient.target("http://127.0.0.1:4444/1");
            }
        };

        IronProject project = ironClient.getProject(System.getenv("IRON_MQ_PROJECT_ID"), System.getenv("IRON_MQ_TOKEN"));

        IronQueue pushQueue = project.newMulticastQueue(
            "tests.unit-test-" + LocalDateTime.now().toString().replace(":", ""),
            Arrays.asList(new IronSubscriber("ironworker:///sample")));

        pushQueue.offer(Json.createObjectBuilder().add("key", "value").build());

        Thread.sleep(2000);

        pushQueue.delete();
    }

}
