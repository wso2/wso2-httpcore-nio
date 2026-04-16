/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 */

package org.apache.http.impl.nio.reactor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.nio.DefaultHttpServerIODispatch;
import org.apache.http.nio.protocol.HttpAsyncService;
import org.apache.http.nio.protocol.UriHttpAsyncRequestHandlerMapper;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.IOReactorStatus;
import org.apache.http.nio.reactor.ListenerEndpoint;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests verifying WARN log emission on abnormal IOReactor shutdown (Issue #4766).
 *
 * <p>Bug: when a worker terminates or the main selector loop throws an
 * IOReactorException, no WARN was logged, making the root cause invisible.
 * Fix: log.warn(...) added at the catch(IOReactorException) site in execute().
 *
 * <p>Integration tests inject a mock {@link Log} into the private static final
 * field via sun.misc.Unsafe (requires --add-opens java.base/sun.misc=ALL-UNNAMED
 * in the Surefire JVM args, already configured in the module pom.xml).
 */
public class TestAbstractMultiworkerIOReactorLogging {

    // -----------------------------------------------------------------------
    // Infrastructure — Unsafe-based static field injection
    // -----------------------------------------------------------------------

    private static final Object UNSAFE = getUnsafe();

    /** Obtain sun.misc.Unsafe instance fully through reflection (no direct import). */
    private static Object getUnsafe() {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot obtain sun.misc.Unsafe — ensure --add-opens java.base/sun.misc=ALL-UNNAMED is set", e);
        }
    }

    /**
     * Replace AbstractMultiworkerIOReactor.log with newLog and return the original.
     * Works on private static final fields because Unsafe bypasses all Java access checks.
     */
    private static Log swapLog(Log newLog) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Method staticFieldBase   = unsafeClass.getMethod("staticFieldBase",   Field.class);
        Method staticFieldOffset = unsafeClass.getMethod("staticFieldOffset", Field.class);
        Method getObject  = unsafeClass.getMethod("getObject",  Object.class, long.class);
        Method putObject  = unsafeClass.getMethod("putObject",  Object.class, long.class, Object.class);

        Field logField = AbstractMultiworkerIOReactor.class.getDeclaredField("log");
        Object base    = staticFieldBase.invoke(UNSAFE, logField);
        long   offset  = (Long) staticFieldOffset.invoke(UNSAFE, logField);

        Log old = (Log) getObject.invoke(UNSAFE, base, offset);
        putObject.invoke(UNSAFE, base, offset, newLog);
        return old;
    }

    // -----------------------------------------------------------------------
    // Test fixtures
    // -----------------------------------------------------------------------

    private Log mockLog;
    private Log originalLog;

    @Before
    public void setUp() throws Exception {
        mockLog = Mockito.mock(Log.class);
        Mockito.when(mockLog.isWarnEnabled()).thenReturn(true);
        originalLog = swapLog(mockLog);
    }

    @After
    public void tearDown() throws Exception {
        if (originalLog != null) {
            swapLog(originalLog);
        }
    }

    private static IOEventDispatch createIOEventDispatch() {
        return new DefaultHttpServerIODispatch(
                new HttpAsyncService(
                        new ImmutableHttpProcessor(
                                new ResponseDate(), new ResponseServer(),
                                new ResponseContent(), new ResponseConnControl()),
                        new UriHttpAsyncRequestHandlerMapper()),
                ConnectionConfig.DEFAULT);
    }

    // -----------------------------------------------------------------------
    // Unit tests — Worker inner class (no threading, no log interception needed)
    // -----------------------------------------------------------------------

    /**
     * Worker must store an IOReactorException thrown by the dispatcher without
     * re-throwing it (Exceptions are swallowed; only Errors are re-thrown).
     */
    @Test
    public void testWorkerStoresExceptionOnDispatchFailure() throws Exception {
        IOReactorException expected = new IOReactorException("simulated dispatch failure");
        BaseIOReactor dispatcher = Mockito.mock(BaseIOReactor.class);
        IOEventDispatch eventDispatch = Mockito.mock(IOEventDispatch.class);
        Mockito.doThrow(expected).when(dispatcher).execute(eventDispatch);

        AbstractMultiworkerIOReactor.Worker worker =
                new AbstractMultiworkerIOReactor.Worker(dispatcher, eventDispatch);
        worker.run(); // must not throw

        Assert.assertSame("Worker must store the thrown IOReactorException", expected, worker.getThrowable());
    }

    /**
     * Worker must store AND re-throw an Error.
     * Models the actual customer root cause: NoSuchMethodError from a JDK version mismatch.
     */
    @Test
    public void testWorkerStoresAndRethrowsJvmError() throws Exception {
        // The actual error seen in the customer environment (Issue #4766)
        NoSuchMethodError nsme = new NoSuchMethodError("java.nio.ByteBuffer.clear()Ljava/nio/ByteBuffer;");
        BaseIOReactor dispatcher = Mockito.mock(BaseIOReactor.class);
        IOEventDispatch eventDispatch = Mockito.mock(IOEventDispatch.class);
        Mockito.doThrow(nsme).when(dispatcher).execute(eventDispatch);

        AbstractMultiworkerIOReactor.Worker worker =
                new AbstractMultiworkerIOReactor.Worker(dispatcher, eventDispatch);
        try {
            worker.run();
            Assert.fail("Error must be re-thrown by Worker");
        } catch (NoSuchMethodError e) {
            Assert.assertSame("Re-thrown Error must be the original instance", nsme, e);
        }
        Assert.assertSame("Worker must also store the Error as throwable", nsme, worker.getThrowable());
    }

    /**
     * Worker must store a RuntimeException without re-throwing it.
     */
    @Test
    public void testWorkerStoresRuntimeException() throws Exception {
        RuntimeException rte = new RuntimeException("unexpected runtime failure");
        BaseIOReactor dispatcher = Mockito.mock(BaseIOReactor.class);
        IOEventDispatch eventDispatch = Mockito.mock(IOEventDispatch.class);
        Mockito.doThrow(rte).when(dispatcher).execute(eventDispatch);

        AbstractMultiworkerIOReactor.Worker worker =
                new AbstractMultiworkerIOReactor.Worker(dispatcher, eventDispatch);
        worker.run(); // must not throw

        Assert.assertSame("Worker must store RuntimeException", rte, worker.getThrowable());
    }

    /**
     * Worker's throwable is null when the dispatcher completes normally.
     */
    @Test
    public void testWorkerGetThrowableIsNullAfterNormalCompletion() throws Exception {
        BaseIOReactor dispatcher = Mockito.mock(BaseIOReactor.class);
        IOEventDispatch eventDispatch = Mockito.mock(IOEventDispatch.class);
        // dispatcher.execute() returns normally by default

        AbstractMultiworkerIOReactor.Worker worker =
                new AbstractMultiworkerIOReactor.Worker(dispatcher, eventDispatch);
        worker.run();

        Assert.assertNull("No throwable expected when worker completes normally", worker.getThrowable());
    }

    // -----------------------------------------------------------------------
    // Integration tests — WARN log verification via DefaultListeningIOReactor
    // -----------------------------------------------------------------------

    /**
     * Bug scenario (Issue #4766): binding to an already-occupied port causes
     * an IOReactorException. BEFORE the fix: no WARN was logged.
     * AFTER the fix: log.warn(..., IOReactorException) is called, making the
     * root cause visible in the log stream.
     */
    @Test
    public void testWarnLogEmittedWhenReactorFails() throws Exception {
        // Occupy an ephemeral port
        DefaultListeningIOReactor occupier = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        Thread occupierThread = new Thread(new Runnable() {
            public void run() {
                try { occupier.execute(createIOEventDispatch()); } catch (IOException ignored) {}
            }
        });
        occupierThread.start();
        ListenerEndpoint ep = occupier.listen(new InetSocketAddress(0));
        ep.waitFor();
        final int boundPort = ((InetSocketAddress) ep.getAddress()).getPort();

        // Start a second reactor and attempt to bind the same port → IOReactorException
        DefaultListeningIOReactor victim = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<IOException> thrown = new AtomicReference<IOException>();
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    victim.execute(createIOEventDispatch());
                } catch (IOException ex) {
                    thrown.set(ex);
                    latch.countDown();
                }
            }
        });
        t.start();
        victim.listen(new InetSocketAddress(boundPort)).waitFor();

        Assert.assertTrue("Victim reactor should have failed within 5 s",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(IOReactorStatus.SHUT_DOWN, victim.getStatus());

        // The WARN must have been logged with an IOReactorException
        ArgumentCaptor<Throwable> exCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(mockLog, Mockito.atLeastOnce())
               .warn(Mockito.anyString(), exCaptor.capture());

        boolean warnedWithReactorException = false;
        for (Throwable ex : exCaptor.getAllValues()) {
            if (ex instanceof IOReactorException) {
                warnedWithReactorException = true;
                break;
            }
        }
        Assert.assertTrue(
                "A WARN log containing an IOReactorException must be emitted when reactor fails abnormally",
                warnedWithReactorException);

        occupier.shutdown(1000);
        occupierThread.join(2000);
        t.join(2000);
    }

    /**
     * The WARN message text must identify the shutdown trigger so operators
     * can locate the entry without parsing the stack trace.
     */
    @Test
    public void testWarnMessageTextIdentifiesShutdownTrigger() throws Exception {
        DefaultListeningIOReactor occupier = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        Thread occupierThread = new Thread(new Runnable() {
            public void run() {
                try { occupier.execute(createIOEventDispatch()); } catch (IOException ignored) {}
            }
        });
        occupierThread.start();
        ListenerEndpoint ep = occupier.listen(new InetSocketAddress(0));
        ep.waitFor();
        final int boundPort = ((InetSocketAddress) ep.getAddress()).getPort();

        DefaultListeningIOReactor victim = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try { victim.execute(createIOEventDispatch()); } catch (IOException ex) { latch.countDown(); }
            }
        });
        t.start();
        victim.listen(new InetSocketAddress(boundPort)).waitFor();
        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<String> msgCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockLog, Mockito.atLeastOnce())
               .warn(msgCaptor.capture(), Mockito.any(Throwable.class));

        boolean mentionsShutdown = false;
        for (String msg : msgCaptor.getAllValues()) {
            if (msg.contains("hard shutdown") || msg.contains("IOReactorException")) {
                mentionsShutdown = true;
                break;
            }
        }
        Assert.assertTrue(
                "WARN message must reference 'hard shutdown' or 'IOReactorException'",
                mentionsShutdown);

        occupier.shutdown(1000);
        occupierThread.join(2000);
        t.join(2000);
    }

    /**
     * The IOReactorException thrown from execute() must carry the original cause
     * so the root exception is preserved in the exception chain.
     */
    @Test
    public void testIOReactorExceptionPreservesCauseChain() throws Exception {
        DefaultListeningIOReactor occupier = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        Thread occupierThread = new Thread(new Runnable() {
            public void run() {
                try { occupier.execute(createIOEventDispatch()); } catch (IOException ignored) {}
            }
        });
        occupierThread.start();
        ListenerEndpoint ep = occupier.listen(new InetSocketAddress(0));
        ep.waitFor();
        final int boundPort = ((InetSocketAddress) ep.getAddress()).getPort();

        DefaultListeningIOReactor victim = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<IOException> thrown = new AtomicReference<IOException>();
        Thread t = new Thread(new Runnable() {
            public void run() {
                try { victim.execute(createIOEventDispatch()); } catch (IOException ex) {
                    thrown.set(ex);
                    latch.countDown();
                }
            }
        });
        t.start();
        victim.listen(new InetSocketAddress(boundPort)).waitFor();
        latch.await(5, TimeUnit.SECONDS);

        IOException reactorEx = thrown.get();
        Assert.assertNotNull("An IOException must be thrown from execute()", reactorEx);
        Assert.assertTrue("Exception must be an IOReactorException", reactorEx instanceof IOReactorException);
        Assert.assertNotNull("IOReactorException must carry a cause", reactorEx.getCause());

        occupier.shutdown(1000);
        occupierThread.join(2000);
        t.join(2000);
    }

    /**
     * The audit log must contain the root cause of the failure so that callers
     * can inspect the exception chain after the reactor shuts down.
     */
    @Test
    public void testAuditLogContainsCauseOnAbnormalShutdown() throws Exception {
        DefaultListeningIOReactor occupier = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        Thread occupierThread = new Thread(new Runnable() {
            public void run() {
                try { occupier.execute(createIOEventDispatch()); } catch (IOException ignored) {}
            }
        });
        occupierThread.start();
        ListenerEndpoint ep = occupier.listen(new InetSocketAddress(0));
        ep.waitFor();
        final int boundPort = ((InetSocketAddress) ep.getAddress()).getPort();

        DefaultListeningIOReactor victim = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try { victim.execute(createIOEventDispatch()); } catch (IOException ex) { latch.countDown(); }
            }
        });
        t.start();
        victim.listen(new InetSocketAddress(boundPort)).waitFor();
        latch.await(5, TimeUnit.SECONDS);

        List<ExceptionEvent> auditLog = victim.getAuditLog();
        Assert.assertFalse("Audit log must not be empty after abnormal shutdown", auditLog.isEmpty());
        Assert.assertNotNull("Audit log entry must have a non-null cause", auditLog.get(0).getCause());

        occupier.shutdown(1000);
        occupierThread.join(2000);
        t.join(2000);
    }

    /**
     * Negative test: a graceful shutdown must NOT emit any WARN log.
     * Ensures the fix doesn't introduce spurious warnings under normal operation.
     */
    @Test
    public void testNoWarnLogOnGracefulShutdown() throws Exception {
        DefaultListeningIOReactor ioReactor = new DefaultListeningIOReactor(
                IOReactorConfig.custom().setIoThreadCount(1).build());
        CountDownLatch started = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    started.countDown();
                    ioReactor.execute(createIOEventDispatch());
                } catch (IOException ignored) {}
            }
        });
        t.start();
        started.await(2, TimeUnit.SECONDS);
        Thread.sleep(100); // let the reactor enter its select loop

        ioReactor.shutdown(1000);
        t.join(2000);

        Assert.assertEquals(IOReactorStatus.SHUT_DOWN, ioReactor.getStatus());
        Mockito.verify(mockLog, Mockito.never())
               .warn(Mockito.anyString(), Mockito.any(Throwable.class));
    }
}
