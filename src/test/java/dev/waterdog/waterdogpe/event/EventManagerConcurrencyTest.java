/*
 * Copyright 2026 WaterdogTEAM
 * Licensed under the GNU General Public License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.waterdog.waterdogpe.event;

import dev.waterdog.waterdogpe.ProxyServer;
import dev.waterdog.waterdogpe.utils.config.proxy.ProxyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Every player's packets are handled on that player's netty event loop, so callEvent and
 * subscribe run on many threads at once. Right after a restart the per-event-class handler
 * cache is empty and the first calls all insert into it concurrently.
 */
public class EventManagerConcurrencyTest {

    private static final int THREADS = 8;
    private static final int EVENT_CLASSES = 512;
    private static final int ROUNDS = 20;

    private ProxyServer proxy;
    private final List<EventManager> managers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        this.proxy = mock(ProxyServer.class);
        ProxyConfig config = mock(ProxyConfig.class);
        when(this.proxy.getConfiguration()).thenReturn(config);
        when(config.getIdleThreads()).thenReturn(0);
    }

    @AfterEach
    void tearDown() {
        this.managers.forEach(manager -> manager.getThreadedExecutor().shutdownNow());
    }

    @Test
    void concurrentFirstCallsOfManyEventTypesDoNotCorruptTheHandlerCache() {
        List<Class<? extends Event>> eventClasses = distinctEventClasses(EVENT_CLASSES);
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            for (int round = 0; round < ROUNDS; round++) {
                EventManager eventManager = this.newEventManager();
                Queue<Throwable> errors = runConcurrently(thread -> {
                    // Each thread walks the classes from a different offset so inserts collide.
                    for (int i = 0; i < eventClasses.size(); i++) {
                        Class<? extends Event> eventClass = eventClasses.get((i + thread * 61) % eventClasses.size());
                        eventManager.callEvent(newEvent(eventClass));
                    }
                });
                assertTrue(errors.isEmpty(), "callEvent threw on round " + round + ": " + errors.peek());
            }
        });
    }

    @Test
    void handlersSubscribedWhileEventsAreCalledAreNeverLost() {
        List<Class<? extends Event>> eventClasses = distinctEventClasses(EVENT_CLASSES);
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            for (int round = 0; round < ROUNDS; round++) {
                EventManager eventManager = this.newEventManager();
                AtomicInteger calls = new AtomicInteger();
                Queue<Throwable> errors = runConcurrently(thread -> {
                    for (int i = 0; i < eventClasses.size(); i++) {
                        Class<? extends Event> eventClass = eventClasses.get((i + thread * 61) % eventClasses.size());
                        if (thread % 2 == 0) {
                            // One subscriber per (thread, class), so every subscribe must survive.
                            int priority = thread / 2 % EventPriority.values().length;
                            eventManager.subscribe((Class<Event>) eventClass, event -> calls.incrementAndGet(), EventPriority.values()[priority]);
                        } else {
                            eventManager.callEvent(newEvent(eventClass));
                        }
                    }
                });
                assertTrue(errors.isEmpty(), "subscribe/callEvent threw on round " + round + ": " + errors.peek());

                calls.set(0);
                for (Class<? extends Event> eventClass : eventClasses) {
                    eventManager.callEvent(newEvent(eventClass));
                }
                assertEquals(eventClasses.size() * (THREADS / 2), calls.get(), "subscribed handlers went missing on round " + round);
            }
        });
    }

    private EventManager newEventManager() {
        EventManager eventManager = new EventManager(this.proxy);
        this.managers.add(eventManager);
        return eventManager;
    }

    private static Queue<Throwable> runConcurrently(ThreadBody body) throws InterruptedException {
        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        CyclicBarrier start = new CyclicBarrier(THREADS);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            int thread = t;
            threads.add(new Thread(() -> {
                try {
                    start.await();
                    body.run(thread);
                } catch (Throwable error) {
                    errors.add(error);
                }
            }, "event-race-" + t));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        return errors;
    }

    @FunctionalInterface
    private interface ThreadBody {
        void run(int thread) throws Exception;
    }

    private static Event newEvent(Class<? extends Event> eventClass) {
        try {
            return eventClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The handler cache is keyed by event class, so a realistic race needs many classes.
     * Defining {@link ProbeEvent} in separate class loaders gives a distinct Class each time.
     */
    @SuppressWarnings("unchecked")
    private static List<Class<? extends Event>> distinctEventClasses(int count) {
        byte[] bytes;
        String resource = ProbeEvent.class.getName().replace('.', '/') + ".class";
        try (InputStream in = EventManagerConcurrencyTest.class.getClassLoader().getResourceAsStream(resource)) {
            bytes = in.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        List<Class<? extends Event>> classes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            classes.add((Class<? extends Event>) new ProbeLoader(bytes).loadClass());
        }
        return classes;
    }

    private static final class ProbeLoader extends ClassLoader {
        private final byte[] bytes;

        ProbeLoader(byte[] bytes) {
            super(EventManagerConcurrencyTest.class.getClassLoader());
            this.bytes = bytes;
        }

        Class<?> loadClass() {
            return this.defineClass(ProbeEvent.class.getName(), this.bytes, 0, this.bytes.length);
        }
    }

    public static class ProbeEvent extends Event {
        public ProbeEvent() {
        }
    }
}
