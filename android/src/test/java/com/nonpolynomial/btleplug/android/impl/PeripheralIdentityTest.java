package com.nonpolynomial.btleplug.android.impl;
import android.bluetooth.*;
import io.github.gedgygedgy.rust.future.FutureException;
import io.github.gedgygedgy.rust.task.Waker;
import java.lang.reflect.*;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PeripheralIdentityTest {
    static final UUID A = new UUID(0, 1), B = new UUID(0, 2), C = new UUID(0, 3), D = new UUID(0, 4);
    static final UUID CCCD = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb");
    static Object field(Peripheral owner, String name) throws Exception {
        Field f = Peripheral.class.getDeclaredField(name); f.setAccessible(true); return f.get(owner);
    }
    static void set(Peripheral owner, String name, Object value) throws Exception {
        Field f = Peripheral.class.getDeclaredField(name); f.setAccessible(true); f.set(owner, value);
    }
    static Peripheral owner(BluetoothGatt gatt) throws Exception {
        Peripheral p = mock(Peripheral.class);
        doCallRealMethod().when(p).setCharacteristicNotification(any(),any(),anyBoolean(),anyBoolean());
        set(p, "commandQueue", new LinkedList<>()); set(p, "notificationStreams", new LinkedList<>());
        set(p, "gatt", gatt); set(p, "connected", true); return p;
    }
    static void services(BluetoothGatt g, BluetoothGattService... services) {
        when(g.getServices()).thenReturn(List.of(services));
    }
    static BluetoothGattCharacteristic characteristic() {
        var c = mock(BluetoothGattCharacteristic.class); when(c.getUuid()).thenReturn(C); return c;
    }
    static BluetoothGattService service(UUID id, BluetoothGattCharacteristic... cs) {
        var s = mock(BluetoothGattService.class); when(s.getUuid()).thenReturn(id);
        when(s.getCharacteristics()).thenReturn(List.of(cs));
        for (var c : cs) when(c.getService()).thenReturn(s); return s;
    }
    static BluetoothGattDescriptor descriptor(BluetoothGattCharacteristic c, UUID id) {
        var d = mock(BluetoothGattDescriptor.class); when(d.getUuid()).thenReturn(id);
        when(d.getCharacteristic()).thenReturn(c); when(c.getDescriptors()).thenReturn(List.of(d)); return d;
    }
    @Test public void readSelectsRequestedServiceAndRejectsWrongServiceCallback() throws Exception {
        for (boolean wrong : List.of(false, true)) {
            var g = mock(BluetoothGatt.class); var a = characteristic(); var b = characteristic();
            services(g, service(A,a), service(B,b));
            when(g.readCharacteristic(b)).thenReturn(true); when(b.getValue()).thenReturn(new byte[]{0,(byte)255});
            var p = owner(g); doCallRealMethod().when(p).read(B,C); var result = p.read(B,C);
            verify(g).readCharacteristic(b); verify(g, never()).readCharacteristic(a);
            ((BluetoothGattCallback)field(p,"commandCallback")).onCharacteristicRead(g, wrong ? a : b, 0);
            if (wrong) assertTrue(assertThrows(FutureException.class, () -> result.poll(mock(Waker.class)).get()).getCause() instanceof UnexpectedCharacteristicException);
            else assertArrayEquals(new byte[]{0,(byte)255}, result.poll(mock(Waker.class)).get());
        }
    }
    @Test public void bothWriteModesSelectRequestedService() throws Exception {
        for (int mode : List.of(1,2)) {
            var g = mock(BluetoothGatt.class); var a = characteristic(); var b = characteristic();
            services(g, service(A,a),service(B,b)); when(g.writeCharacteristic(b)).thenReturn(true);
            var p = owner(g); byte[] bytes = {0,(byte)255}; doCallRealMethod().when(p).write(B,C,bytes,mode);
            var result = p.write(B,C,bytes,mode); verify(g).writeCharacteristic(b); verify(g,never()).writeCharacteristic(a);
            verify(b).setValue(bytes); verify(b).setWriteType(mode);
            ((BluetoothGattCallback)field(p,"commandCallback")).onCharacteristicWrite(g,b,0);
            assertNull(result.poll(mock(Waker.class)).get());
        }
    }
    @Test public void descriptorReadAndWriteSelectRequestedService() throws Exception {
        for (boolean write : List.of(false,true)) {
            var g = mock(BluetoothGatt.class); var a = characteristic(); var b = characteristic();
            var da = descriptor(a,D); var db = descriptor(b,D); when(db.getValue()).thenReturn(new byte[]{42});
            services(g, service(A,a),service(B,b));
            when(g.readDescriptor(db)).thenReturn(true); when(g.writeDescriptor(db)).thenReturn(true);
            var p = owner(g);
            if (write) {
                byte[] bytes = {43}; doCallRealMethod().when(p).writeDescriptor(B,C,D,bytes);
                var result = p.writeDescriptor(B,C,D,bytes); verify(g).writeDescriptor(db); verify(g,never()).writeDescriptor(da);
                ((BluetoothGattCallback)field(p,"commandCallback")).onDescriptorWrite(g,db,0);
                assertNull(result.poll(mock(Waker.class)).get());
            } else {
                doCallRealMethod().when(p).readDescriptor(B,C,D); var result = p.readDescriptor(B,C,D);
                verify(g).readDescriptor(db); verify(g,never()).readDescriptor(da);
                ((BluetoothGattCallback)field(p,"commandCallback")).onDescriptorRead(g,db,0);
                assertArrayEquals(new byte[]{42},result.poll(mock(Waker.class)).get());
            }
        }
    }
    @Test public void notificationSetupAndTeardownSelectRequestedService() throws Exception {
        for (boolean enable : List.of(false,true)) {
            var g = mock(BluetoothGatt.class); var a = characteristic(); var b = characteristic();
            var da = descriptor(a,CCCD); var db = descriptor(b,CCCD);
            services(g, service(A,a),service(B,b));
            when(g.setCharacteristicNotification(b,enable)).thenReturn(true); when(g.writeDescriptor(db)).thenReturn(true);
            var p = owner(g); doCallRealMethod().when(p).setCharacteristicNotification(B,C,enable);
            var result = p.setCharacteristicNotification(B,C,enable);
            verify(g).setCharacteristicNotification(b,enable); verify(g,never()).setCharacteristicNotification(a,enable);
            verify(g).writeDescriptor(db); verify(g,never()).writeDescriptor(da);
            ((BluetoothGattCallback)field(p,"commandCallback")).onDescriptorWrite(g,db,0);
            assertNull(result.poll(mock(Waker.class)).get());
        }
    }
    @Test public void duplicatesRemainRejectedBeforeGattOperation() throws Exception {
        for (int kind = 0; kind < 3; kind++) {
            var g = mock(BluetoothGatt.class); var c = characteristic(); var p = owner(g);
            if (kind == 0) services(g, service(A,c,characteristic()));
            if (kind == 1) services(g, service(A,c),service(A,characteristic()));
            if (kind == 2) {
                var d1 = descriptor(c,D); var d2 = descriptor(c,D); when(c.getDescriptors()).thenReturn(List.of(d1,d2));
                services(g, service(A,c));
            }
            if (kind == 2) {
                doCallRealMethod().when(p).readDescriptor(A,C,D);
                assertTrue(assertThrows(FutureException.class, () -> p.readDescriptor(A,C,D).poll(mock(Waker.class)).get()).getCause() instanceof UnexpectedCharacteristicException);
            } else {
                doCallRealMethod().when(p).read(A,C);
                assertTrue(assertThrows(FutureException.class, () -> p.read(A,C).poll(mock(Waker.class)).get()).getCause() instanceof UnexpectedCharacteristicException);
            }
            verify(g,never()).readCharacteristic(any()); verify(g,never()).readDescriptor(any());
        }
    }
    @Test public void notificationSnapshotCopiesPayloadAndRetainsSourceServiceIdentity() throws Exception {
        var g = mock(BluetoothGatt.class); var original = characteristic(); var source = service(B,original);
        byte[] value = {0,(byte)255}; when(original.getValue()).thenReturn(value);
        when(source.getType()).thenReturn(BluetoothGattService.SERVICE_TYPE_PRIMARY);
        var p = owner(g); doCallRealMethod().when(p).getNotifications(); var stream = p.getNotifications();
        Class<?> callbackClass = Class.forName(Peripheral.class.getName()+"$Callback");
        var constructor = callbackClass.getDeclaredConstructor(Peripheral.class); constructor.setAccessible(true);
        var callback = (BluetoothGattCallback)constructor.newInstance(p);
        var serviceArgs = new ArrayList<List<?>>();
        try (var characteristics = mockConstruction(BluetoothGattCharacteristic.class);
             var services = mockConstruction(BluetoothGattService.class, (copy, context) -> serviceArgs.add(context.arguments()))) {
            callback.onCharacteristicChanged(g,original);
            var copied = characteristics.constructed().get(0); var copiedService = services.constructed().get(0);
            var bytes = org.mockito.ArgumentCaptor.forClass(byte[].class); verify(copied).setValue(bytes.capture());
            value[0] = 42; assertArrayEquals(new byte[]{0,(byte)255},bytes.getValue()); assertNotSame(value,bytes.getValue());
            assertEquals(B,serviceArgs.get(0).get(0)); verify(copiedService).addCharacteristic(copied);
            assertSame(copied, stream.pollNext(mock(Waker.class)).get().get());
        }
    }

    static BluetoothGattCallback callback(Peripheral p) throws Exception {
        var constructor = Class.forName(Peripheral.class.getName()+"$Callback").getDeclaredConstructor(Peripheral.class);
        constructor.setAccessible(true); return (BluetoothGattCallback)constructor.newInstance(p);
    }
    @Test public void modernReadsUseCopiedEventValuesAndRejectWrongServiceAndFailure() throws Exception {
        for (boolean descriptorRead : List.of(false,true)) {
            for (int outcome : List.of(0,1,2)) {
                var g = mock(BluetoothGatt.class); var a = characteristic(); var b = characteristic();
                var da = descriptor(a,D); var db = descriptor(b,D);
                services(g,service(A,a),service(B,b));
                when(g.readCharacteristic(b)).thenReturn(true); when(g.readDescriptor(db)).thenReturn(true);
                var p = owner(g); byte[] supplied = {0,(byte)255,(byte)128};
                io.github.gedgygedgy.rust.future.Future<byte[]> result;
                if (descriptorRead) {
                    doCallRealMethod().when(p).readDescriptor(B,C,D); result=p.readDescriptor(B,C,D);
                    callback(p).onDescriptorRead(g,outcome==1 ? da : db,outcome==2 ? 5 : 0,supplied);
                } else {
                    doCallRealMethod().when(p).read(B,C); result=p.read(B,C);
                    callback(p).onCharacteristicRead(g,outcome==1 ? a : b,supplied,outcome==2 ? 5 : 0);
                }
                supplied[0]=42;
                if (outcome==0) {
                    var value=result.poll(mock(Waker.class)).get();
                    assertArrayEquals(new byte[]{0,(byte)255,(byte)128},value); assertNotSame(supplied,value);
                } else {
                    var failure=assertThrows(FutureException.class,()->result.poll(mock(Waker.class)).get());
                    if (outcome==1) assertTrue(failure.getCause() instanceof UnexpectedCharacteristicException);
                    else assertTrue(failure.getCause().getMessage().contains("status: 5"));
                }
                verify(a,never()).getValue(); verify(b,never()).getValue();
                verify(da,never()).getValue(); verify(db,never()).getValue();
            }
        }
    }
    @Test public void modernNotificationUsesEventValueWithoutReadingMutableField() throws Exception {
        var g=mock(BluetoothGatt.class); var original=characteristic(); service(B,original);
        var p=owner(g); doCallRealMethod().when(p).getNotifications(); var stream=p.getNotifications();
        byte[] supplied={0,(byte)255};
        try (var cs=mockConstruction(BluetoothGattCharacteristic.class);
             var ss=mockConstruction(BluetoothGattService.class)) {
            callback(p).onCharacteristicChanged(g,original,supplied);
            var copy=cs.constructed().get(0); var bytes=org.mockito.ArgumentCaptor.forClass(byte[].class);
            verify(copy).setValue(bytes.capture()); supplied[0]=42;
            assertArrayEquals(new byte[]{0,(byte)255},bytes.getValue()); assertNotSame(supplied,bytes.getValue());
            verify(original,never()).getValue(); verify(ss.constructed().get(0)).addCharacteristic(copy);
            assertSame(copy,stream.pollNext(mock(Waker.class)).get().get());
        }
    }

    @Test public void duplicateCccdFailsBeforeNotificationTouchesGatt() throws Exception {
        var g = mock(BluetoothGatt.class); var c = characteristic();
        var d1 = descriptor(c,CCCD); var d2 = descriptor(c,CCCD); when(c.getDescriptors()).thenReturn(List.of(d1,d2));
        services(g,service(A,c)); var p = owner(g);
        doCallRealMethod().when(p).setCharacteristicNotification(A,C,true);
        var result = p.setCharacteristicNotification(A,C,true);
        assertTrue(assertThrows(FutureException.class, () -> result.poll(mock(Waker.class)).get()).getCause() instanceof UnexpectedCharacteristicException);
        verify(g,never()).setCharacteristicNotification(any(),anyBoolean()); verify(g,never()).writeDescriptor(any());
    }


    @Test public void explicitCompatibilitySetsLocalRoutingWithoutCccdAndRetainsErrors() throws Exception {
        var status = UUID.fromString("00010203-0405-0607-0809-0a0b0c0d1911");
        for (boolean enable : List.of(true,false)) {
            for (boolean accepted : List.of(true,false)) {
                var g = mock(BluetoothGatt.class); var c = mock(BluetoothGattCharacteristic.class);
                when(c.getUuid()).thenReturn(status); when(c.getProperties()).thenReturn(BluetoothGattCharacteristic.PROPERTY_NOTIFY);
                services(g, service(A,c)); when(g.setCharacteristicNotification(c,enable)).thenReturn(accepted);
                var p = owner(g); doCallRealMethod().when(p).setCharacteristicNotification(A,status,enable,true);
                var result = p.setCharacteristicNotification(A,status,enable,true);
                verify(g).setCharacteristicNotification(c,enable); verify(g,never()).writeDescriptor(any());
                if (accepted) assertNull(result.poll(mock(Waker.class)).get());
                else assertThrows(FutureException.class, () -> result.poll(mock(Waker.class)).get());
            }
        }
    }
    @Test public void compatibilityRejectsNonNotify() throws Exception {
        var status = UUID.fromString("00010203-0405-0607-0809-0a0b0c0d1911");
        var g = mock(BluetoothGatt.class); var c = mock(BluetoothGattCharacteristic.class);
        when(c.getUuid()).thenReturn(status); when(c.getProperties()).thenReturn(BluetoothGattCharacteristic.PROPERTY_INDICATE);
        services(g, service(A,c)); var p = owner(g); doCallRealMethod().when(p).setCharacteristicNotification(A,status,true,true);
        var result = p.setCharacteristicNotification(A,status,true,true);
        assertThrows(FutureException.class, () -> result.poll(mock(Waker.class)).get());
        verify(g,never()).setCharacteristicNotification(any(),anyBoolean());
    }
    @Test public void statusUuidUsesStandardCccdByDefault() throws Exception {
        var status = UUID.fromString("00010203-0405-0607-0809-0a0b0c0d1911");
        var g = mock(BluetoothGatt.class); var c = mock(BluetoothGattCharacteristic.class);
        when(c.getUuid()).thenReturn(status); when(c.getProperties()).thenReturn(BluetoothGattCharacteristic.PROPERTY_NOTIFY);
        services(g, service(A,c)); var p = owner(g);
        doCallRealMethod().when(p).setCharacteristicNotification(A,status,true);
        var result = p.setCharacteristicNotification(A,status,true);
        assertThrows(FutureException.class, () -> result.poll(mock(Waker.class)).get());
        verify(g,never()).setCharacteristicNotification(any(),anyBoolean());
    }

}
