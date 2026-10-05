package com.nonpolynomial.btleplug.android.impl;

import android.bluetooth.*;
import java.lang.reflect.*;
import java.util.List;
import java.util.LinkedList;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/** Exercises the actual bundled callback dispatcher without a BLE radio. */
public class PeripheralGenerationTest {
    @Test public void connectionQueryWaitsForInFlightStateTransition() throws Exception {
        Peripheral owner = mock(Peripheral.class);
        doCallRealMethod().when(owner).isConnected();
        AtomicReference<Boolean> observed = new AtomicReference<>();
        CountDownLatch entering = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            entering.countDown();
            observed.set(owner.isConnected());
        });
        Thread.State duringTransition;
        synchronized (owner) {
            // Binder callbacks update connected under this monitor. Model a
            // transition that has entered the callback but not published yet.
            set(owner, "connected", false);
            reader.start();
            org.junit.Assert.assertTrue(entering.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reader.getState() != Thread.State.BLOCKED && reader.isAlive()
                    && System.nanoTime() < deadline) Thread.yield();
            duringTransition = reader.getState();
            set(owner, "connected", true);
        }
        reader.join(5000);
        org.junit.Assert.assertFalse(reader.isAlive());
        assertEquals(Thread.State.BLOCKED, duringTransition);
        assertEquals(Boolean.TRUE, observed.get());
    }
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
    private static BluetoothGattCallback callback(Peripheral owner) throws Exception {
        Class<?> type = Class.forName(Peripheral.class.getName() + "$Callback");
        Constructor<?> constructor = type.getDeclaredConstructor(Peripheral.class);
        constructor.setAccessible(true);
        return (BluetoothGattCallback) constructor.newInstance(owner);
    }
    @Test public void staleGattCannotCompleteAnyCurrentCommand() throws Exception {
        Peripheral owner = mock(Peripheral.class);
        BluetoothGatt current = mock(BluetoothGatt.class);
        BluetoothGatt stale = mock(BluetoothGatt.class);
        BluetoothGattCharacteristic characteristic = mock(BluetoothGattCharacteristic.class);
        BluetoothGattDescriptor descriptor = mock(BluetoothGattDescriptor.class);
        Class<?> type = Class.forName(Peripheral.class.getName() + "$CommandCallback");
        BluetoothGattCallback command = (BluetoothGattCallback) mock(type);
        set(owner, "gatt", current);
        set(owner, "commandCallback", command);
        BluetoothGattCallback dispatcher = callback(owner);
        byte[] supplied = {0,(byte)255};
        List<Consumer<BluetoothGatt>> operations = List.of(
            g -> dispatcher.onCharacteristicRead(g, characteristic, 0),
            g -> dispatcher.onCharacteristicRead(g, characteristic, supplied, 0),
            g -> dispatcher.onCharacteristicWrite(g, characteristic, 0),
            g -> dispatcher.onServicesDiscovered(g, 0),
            g -> dispatcher.onDescriptorRead(g, descriptor, 0),
            g -> dispatcher.onDescriptorRead(g, descriptor, 0, supplied),
            g -> dispatcher.onDescriptorWrite(g, descriptor, 0),
            g -> dispatcher.onMtuChanged(g, 64, 0),
            g -> dispatcher.onReadRemoteRssi(g, -42, 0)
        );
        for (Consumer<BluetoothGatt> operation : operations) operation.accept(stale);
        verifyNoInteractions(command, characteristic, descriptor);
        for (Consumer<BluetoothGatt> operation : operations) operation.accept(current);
        verify(command).onCharacteristicRead(current, characteristic, 0);
        verify(command).onCharacteristicRead(current, characteristic, supplied, 0);
        verify(command).onCharacteristicWrite(current, characteristic, 0);
        verify(command).onServicesDiscovered(current, 0);
        verify(command).onDescriptorRead(current, descriptor, 0);
        verify(command).onDescriptorRead(current, descriptor, 0, supplied);
        verify(command).onDescriptorWrite(current, descriptor, 0);
        verify(command).onMtuChanged(current, 64, 0);
        verify(command).onReadRemoteRssi(current, -42, 0);
        verifyNoMoreInteractions(command);
    }
    @Test public void staleNotificationsAreRejectedBeforeReadingTheirPayload() throws Exception {
        Peripheral owner = mock(Peripheral.class);
        BluetoothGatt current = mock(BluetoothGatt.class);
        BluetoothGatt stale = mock(BluetoothGatt.class);
        BluetoothGattCharacteristic characteristic = mock(BluetoothGattCharacteristic.class);
        set(owner, "gatt", current);
        set(owner, "notificationStreams", new LinkedList<>());
        BluetoothGattCallback dispatcher = callback(owner);
        for (int cycle = 0; cycle < 100; cycle++) {
            dispatcher.onCharacteristicChanged(stale, characteristic);
            dispatcher.onCharacteristicChanged(stale, characteristic, null);
        }
        verifyNoInteractions(characteristic);
        assertEquals(0, ((LinkedList<?>) get(owner, "notificationStreams")).size());
        set(owner, "gatt", null);
        dispatcher.onCharacteristicChanged(current, characteristic);
        verifyNoInteractions(characteristic);
    }
    @Test public void staleConnectionParametersCannotMutateTheNewGeneration() throws Exception {
        Peripheral owner = mock(Peripheral.class);
        BluetoothGatt current = mock(BluetoothGatt.class);
        BluetoothGatt stale = mock(BluetoothGatt.class);
        set(owner, "gatt", current);
        BluetoothGattCallback dispatcher = callback(owner);
        Method update = dispatcher.getClass().getDeclaredMethod("onConnectionUpdated",
                BluetoothGatt.class, int.class, int.class, int.class, int.class);
        update.setAccessible(true);
        update.invoke(dispatcher, current, 24, 0, 400, 0);
        update.invoke(dispatcher, stale, 99, 7, 900, 0);
        assertEquals(24, get(owner, "connectionInterval"));
        assertEquals(0, get(owner, "connectionLatency"));
        assertEquals(400, get(owner, "supervisionTimeout"));
    }
    @Test public void hundredStreamReplacementsClosePreviousGenerationWithoutGc() throws Exception {
        Peripheral owner = mock(Peripheral.class);
        set(owner, "notificationStreams", new LinkedList<>());
        doCallRealMethod().when(owner).getNotifications();
        io.github.gedgygedgy.rust.stream.QueueStream<?> previous = null;
        for (int cycle = 0; cycle < 100; cycle++) {
            var next = (io.github.gedgygedgy.rust.stream.QueueStream<?>) owner.getNotifications();
            if (previous != null) org.junit.Assert.assertTrue(previous.isFinished());
            assertEquals(1, ((LinkedList<?>) get(owner, "notificationStreams")).size());
            previous = next;
        }
        BluetoothGatt current = mock(BluetoothGatt.class);
        set(owner, "gatt", current);
        set(owner, "adapter", new Adapter() {
            @Override public void onConnectionStateChanged(String address, boolean connected) {}
        });
        set(owner, "device", mock(BluetoothDevice.class));
        callback(owner).onConnectionStateChange(current, 0, BluetoothGatt.STATE_DISCONNECTED);
        org.junit.Assert.assertTrue(previous.isFinished());
        assertEquals(0, ((LinkedList<?>) get(owner, "notificationStreams")).size());
        verify(current).close();
        org.junit.Assert.assertNull(get(owner, "gatt"));
    }

    @Test public void remoteCloseFailureStillPublishesLossAndRetainsGattForRecovery() throws Exception {
        try (var logs = mockStatic(android.util.Log.class)) {
            Peripheral owner = mock(Peripheral.class);
            BluetoothGatt current = mock(BluetoothGatt.class);
            AtomicInteger losses = new AtomicInteger();
            set(owner, "gatt", current);
            set(owner, "connected", true);
            set(owner, "commandQueue", new LinkedList<>());
            set(owner, "notificationStreams", new LinkedList<>());
            set(owner, "device", mock(BluetoothDevice.class));
            set(owner, "adapter", new Adapter() {
                @Override public void onConnectionStateChanged(String address, boolean connected) {
                    org.junit.Assert.assertFalse(connected);
                    losses.incrementAndGet();
                }
            });
            doCallRealMethod().when(owner).getNotifications();
            doCallRealMethod().when(owner).disconnect();
            var stream = (io.github.gedgygedgy.rust.stream.QueueStream<?>) owner.getNotifications();
            doThrow(new SecurityException("Permission revoked during close")).when(current).close();
            callback(owner).onConnectionStateChange(current, 0, BluetoothGatt.STATE_DISCONNECTED);
            assertEquals(1, losses.get());
            org.junit.Assert.assertFalse((boolean) get(owner, "connected"));
            org.junit.Assert.assertTrue(stream.isFinished());
            assertEquals(0, ((LinkedList<?>) get(owner, "notificationStreams")).size());
            org.junit.Assert.assertSame(current, get(owner, "gatt"));
            doNothing().when(current).close();
            org.junit.Assert.assertNull(owner.disconnect().poll(mock(io.github.gedgygedgy.rust.task.Waker.class)).get());
            org.junit.Assert.assertNull(get(owner, "gatt"));
            verify(current, times(2)).close();
            verify(current).disconnect();
            logs.verify(() -> android.util.Log.e(anyString(), eq("Unable to close remotely disconnected GATT"), any(Throwable.class)));
        }
    }

    @Test public void adapterEventFailureCannotEscapeTheBinderCallback() throws Exception {
        try (var logs = mockStatic(android.util.Log.class)) {
            Peripheral owner = mock(Peripheral.class);
            BluetoothGatt current = mock(BluetoothGatt.class);
            AtomicInteger events = new AtomicInteger();
            set(owner, "gatt", current);
            set(owner, "notificationStreams", new LinkedList<>());
            set(owner, "device", mock(BluetoothDevice.class));
            set(owner, "adapter", new Adapter() {
                @Override public void onConnectionStateChanged(String address, boolean connected) {
                    events.incrementAndGet();
                    throw new IllegalStateException("Native event sink unavailable");
                }
            });
            var dispatch = callback(owner);
            dispatch.onConnectionStateChange(current, 0, BluetoothGatt.STATE_CONNECTED);
            org.junit.Assert.assertTrue((boolean) get(owner, "connected"));
            dispatch.onConnectionStateChange(current, 0, BluetoothGatt.STATE_DISCONNECTED);
            assertEquals(2, events.get());
            org.junit.Assert.assertNull(get(owner, "gatt"));
            org.junit.Assert.assertFalse((boolean) get(owner, "connected"));
            verify(current).close();
            logs.verify(() -> android.util.Log.e(anyString(), eq("Unexpected exception dispatching adapterConnected"), any(Throwable.class)));
            logs.verify(() -> android.util.Log.e(anyString(), eq("Unexpected exception dispatching adapterDisconnected"), any(Throwable.class)));
        }
    }

}
