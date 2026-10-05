package com.nonpolynomial.btleplug.android.impl;

import android.bluetooth.BluetoothGatt;
import java.lang.reflect.*;
import java.util.LinkedList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import io.github.gedgygedgy.rust.future.SimpleFuture;
import io.github.gedgygedgy.rust.future.FutureException;
import io.github.gedgygedgy.rust.task.Waker;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PeripheralCancellationTest {
    private static void set(Peripheral owner, String name, Object value) throws Exception {
        Field field = Peripheral.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(owner, value);
    }
    private static Object get(Peripheral owner, String name) throws Exception {
        Field field = Peripheral.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }
    private static Peripheral owner() throws Exception {
        Peripheral owner = mock(Peripheral.class);
        set(owner, "commandQueue", new LinkedList<>());
        set(owner, "notificationStreams", new LinkedList<>());
        doCallRealMethod().when(owner).disconnect();
        return owner;
    }
    private static void enqueue(Peripheral owner, SimpleFuture<?> future, Runnable operation) throws Exception {
        Method method = Peripheral.class.getDeclaredMethod("queueCommand", SimpleFuture.class, Runnable.class);
        method.setAccessible(true);
        method.invoke(owner, future, operation);
    }
    @Test public void disconnectPreemptsStalledCommandCancelsRetryAndSkipsQueuedWork() throws Exception {
        Peripheral owner = owner();
        BluetoothGatt gatt = mock(BluetoothGatt.class);
        set(owner, "gatt", gatt);
        set(owner, "connected", true);
        ScheduledFuture<?> retry = mock(ScheduledFuture.class);
        set(owner, "pendingRetry", retry);
        SimpleFuture<Void> active = new SimpleFuture<>();
        Waker pending = mock(Waker.class);
        assertNull(active.poll(pending));
        enqueue(owner, active, () -> {}); // Controlled OS operation never responds.
        SimpleFuture<Void> queued = new SimpleFuture<>();
        AtomicInteger calls = new AtomicInteger();
        enqueue(owner, queued, calls::incrementAndGet);
        assertNull(owner.disconnect().poll(mock(Waker.class)).get());
        assertThrows(FutureException.class, () -> active.poll(mock(Waker.class)).get());
        assertThrows(FutureException.class, () -> queued.poll(mock(Waker.class)).get());
        verify(pending).wake();
        verifyNoMoreInteractions(pending);
        verify(retry).cancel(false);
        verify(gatt).disconnect();
        verify(gatt).close();
        assertEquals(0, calls.get());
        assertNull(get(owner, "gatt"));
        assertNull(get(owner, "activeFuture"));
        assertNull(get(owner, "pendingRetry"));
        assertEquals(false, get(owner, "executingCommand"));
        // Even a retry that was already runnable cannot resurrect the cancelled connect.
        Method attempt = Peripheral.class.getDeclaredMethod("attemptConnect", SimpleFuture.class, int.class);
        attempt.setAccessible(true);
        attempt.invoke(owner, active, 1);
        verifyNoMoreInteractions(gatt);
        assertNull(owner.disconnect().poll(mock(Waker.class)).get());
        verifyNoMoreInteractions(gatt);
    }
    @Test public void closeFailureIsReportedAndRetainsGattForRecovery() throws Exception {
        Peripheral owner = owner();
        BluetoothGatt gatt = mock(BluetoothGatt.class);
        set(owner, "gatt", gatt);
        enqueue(owner, new SimpleFuture<>(), () -> {});
        doThrow(new IllegalStateException("close failed")).when(gatt).close();
        assertThrows(FutureException.class, () -> owner.disconnect().poll(mock(Waker.class)).get());
        assertSame(gatt, get(owner, "gatt"));
        doNothing().when(gatt).close();
        assertNull(owner.disconnect().poll(mock(Waker.class)).get());
        assertNull(get(owner, "gatt"));
    }
    @Test public void firstTerminalFutureResultWinsAndReleasesOnlyOneWaker() {
        SimpleFuture<Integer> future = new SimpleFuture<>();
        Waker pending = mock(Waker.class);
        assertNull(future.poll(pending));
        future.wakeWithThrowable(new IllegalStateException("cancelled"));
        future.wake(42);
        assertThrows(FutureException.class, () -> future.poll(mock(Waker.class)).get());
        verify(pending).wake();
        verifyNoMoreInteractions(pending);
    }
    @Test public void ordinaryCommandFailurePreservesFifoAndLateCompletionIsIgnored() throws Exception {
        Peripheral owner = owner();
        SimpleFuture<Integer> first = new SimpleFuture<>();
        SimpleFuture<Integer> second = new SimpleFuture<>();
        AtomicInteger order = new AtomicInteger();
        enqueue(owner, first, () -> assertEquals(1, order.incrementAndGet()));
        enqueue(owner, second, () -> assertEquals(2, order.incrementAndGet()));
        assertEquals(1, order.get());
        Method operation = Peripheral.class.getDeclaredMethod("asyncWithFuture", SimpleFuture.class, Runnable.class);
        operation.setAccessible(true);
        operation.invoke(owner, first, (Runnable) () -> { throw new IllegalStateException("read failed"); });
        assertEquals(2, order.get());
        assertThrows(FutureException.class, () -> first.poll(mock(Waker.class)).get());
        Method complete = Peripheral.class.getDeclaredMethod("wakeCommand", SimpleFuture.class, Object.class);
        complete.setAccessible(true);
        complete.invoke(owner, first, 99); // Old completion cannot advance the newer command.
        assertSame(second, get(owner, "activeFuture"));
        complete.invoke(owner, second, 42);
        assertEquals(Integer.valueOf(42), second.poll(mock(Waker.class)).get());
        assertEquals(false, get(owner, "executingCommand"));
        assertNull(get(owner, "activeFuture"));
    }

    @Test public void idleConnectedGattClosesWithoutWaitingForAnOsCallback() throws Exception {
        Peripheral owner = owner();
        BluetoothGatt gatt = mock(BluetoothGatt.class);
        set(owner, "gatt", gatt);
        set(owner, "connected", true);
        assertNull(owner.disconnect().poll(mock(Waker.class)).get());
        verify(gatt).disconnect();
        verify(gatt).close();
        assertNull(get(owner, "gatt"));
        assertEquals(false, get(owner, "connected"));
    }

}
