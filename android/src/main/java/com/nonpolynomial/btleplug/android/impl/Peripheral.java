package com.nonpolynomial.btleplug.android.impl;

import android.annotation.SuppressLint;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCallback;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattDescriptor;
import android.bluetooth.BluetoothGattService;
import android.os.SystemClock;
import android.util.Log;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.github.gedgygedgy.rust.future.Future;
import io.github.gedgygedgy.rust.stream.QueueStream;
import io.github.gedgygedgy.rust.future.SimpleFuture;
import io.github.gedgygedgy.rust.stream.Stream;

@SuppressWarnings("unused") // Native code uses this class.
class Peripheral {
    private static final String TAG = "Peripheral";
    private static final UUID CLIENT_CHARACTERISTIC_CONFIGURATION_DESCRIPTOR = new UUID(0x00002902_0000_1000L, 0x8000_00805f9b34fbL);

    // 0x3E is the HCI reason for a connection that failed to be established; Android usually
    // surfaces it as its generic GATT_ERROR (133), which is not itself a public API constant.
    // Both can be reported for a connection attempt that never reached STATE_CONNECTED, typically
    // because the peripheral missed the first connection events. connect() retries on these
    // instead of surfacing a spurious failure, but only when the failed attempt was fast: on
    // Android <= 14 a ~30s direct-connect timeout is also reported as 133, and retrying that would
    // turn a connect to an absent device into a ~90s wait.
    static final int GATT_ERROR = 133;
    static final int HCI_ERR_CONNECTION_FAILED_ESTABLISHMENT = 0x3e;
    static final int MAX_CONNECT_RETRIES = 2; // up to 3 total connect attempts
    static final long CONNECT_RETRY_DELAY_MS = 200;
    static final long CONNECT_RETRY_MAX_ELAPSED_MS = 10_000;

    // Library-owned scheduler for connect retries: deliberately not tied to any app's main
    // looper, since an app that blocks its main thread while awaiting connect() would otherwise
    // deadlock waiting for its own looper to run the retry.
    private static final ScheduledExecutorService RETRY_EXECUTOR = retryExecutor();
    private static ScheduledExecutorService retryExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "btleplug-connect-retry");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private final BluetoothDevice device;
    private final Adapter adapter;
    private BluetoothGatt gatt;
    private final Callback callback;
    private boolean connected = false;
    // Set for the duration of a single Callback.onConnectionStateChange dispatch to suppress the
    // adapter's DeviceDisconnected notification when that DISCONNECTED is just a retryable failed
    // connection attempt (see attemptConnect) rather than a real disconnect of a connected device.
    private boolean suppressDisconnectNotification = false;

    // Cached connection parameters from onConnectionUpdated callback
    private int connectionInterval = -1;  // in 1.25ms units
    private int connectionLatency = -1;
    private int supervisionTimeout = -1;  // in 10ms units

    private static final class QueuedCommand {
        final SimpleFuture<?> future;
        final Runnable callback;
        QueuedCommand(SimpleFuture<?> future, Runnable callback) {
            this.future = future;
            this.callback = callback;
        }
    }
    private final Queue<QueuedCommand> commandQueue = new LinkedList<>();
    private SimpleFuture<?> activeFuture;
    private ScheduledFuture<?> pendingRetry;
    private final LinkedList<WeakReference<QueueStream<BluetoothGattCharacteristic>>> notificationStreams = new LinkedList<>();
    private boolean executingCommand = false;
    private CommandCallback commandCallback;

    public Peripheral(Adapter adapter, String address) {
        BluetoothAdapter bluetoothAdapter = BluetoothAdapter.getDefaultAdapter();
        if (bluetoothAdapter == null) {
            throw new NoBluetoothAdapterException();
        }
        this.device = bluetoothAdapter.getRemoteDevice(address);
        this.adapter = adapter;
        this.callback = new Callback();
    }

    @SuppressLint("MissingPermission")
    public Future<Void> connect() {
        SimpleFuture<Void> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> this.attemptConnect(future, 0));
        }
        return future;
    }

    /**
     * Pure retry decision for a failed connect attempt, package-private for unit testing.
     *
     * @param status status reported by onConnectionStateChange
     * @param attempt zero-based index of the attempt that just failed
     * @param attemptElapsedMs wall-clock duration of the attempt that just failed
     */
    static boolean shouldRetryConnect(int status, int attempt, long attemptElapsedMs) {
        if (attempt >= MAX_CONNECT_RETRIES) {
            return false;
        }
        if (status != GATT_ERROR && status != HCI_ERR_CONNECTION_FAILED_ESTABLISHMENT) {
            return false;
        }
        return attemptElapsedMs < CONNECT_RETRY_MAX_ELAPSED_MS;
    }

    @SuppressLint("MissingPermission")
    private void attemptConnect(SimpleFuture<Void> future, int attempt) {
        this.asyncWithFuture(future, () -> {
            long attemptStartMs = SystemClock.elapsedRealtime();
            CommandCallback callback = new CommandCallback(future) {
                @Override
                public void onConnectionStateChange(BluetoothGatt gatt, int status, int newState) {
                    Peripheral.this.asyncWithFuture(future, () -> {
                        if (status != BluetoothGatt.GATT_SUCCESS) {
                            long attemptElapsedMs = SystemClock.elapsedRealtime() - attemptStartMs;
                            if (newState == BluetoothGatt.STATE_DISCONNECTED
                                    && Peripheral.shouldRetryConnect(status, attempt, attemptElapsedMs)) {
                                if (Peripheral.this.gatt != null) {
                                    Peripheral.this.gatt.close();
                                    Peripheral.this.gatt = null;
                                }
                                Peripheral.this.commandCallback = null;
                                Peripheral.this.suppressDisconnectNotification = true;
                                Peripheral.this.pendingRetry = RETRY_EXECUTOR.schedule(() -> {
                                    Peripheral.this.dispatchToCommandCallback("connectRetry", () -> {
                                        synchronized (Peripheral.this) {
                                            if (Peripheral.this.activeFuture != future) return;
                                            Peripheral.this.pendingRetry = null;
                                            Peripheral.this.attemptConnect(future, attempt + 1);
                                        }
                                    });
                                }, CONNECT_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                                return;
                            }

                            if (Peripheral.this.gatt != null) {
                                Peripheral.this.gatt.close();
                                Peripheral.this.gatt = null;
                            }
                            Peripheral.this.connected = false;
                            throw new NotConnectedException();
                        }

                        if (newState == BluetoothGatt.STATE_CONNECTED) {
                            Peripheral.this.wakeCommand(future, null);
                        }
                    });
                }
            };

            if (this.connected) {
                Peripheral.this.wakeCommand(future, null);
            } else if (this.gatt == null) {
                try {
                    this.setCommandCallback(callback);
                    this.gatt = this.device.connectGatt(null, false, this.callback);
                    if (this.gatt == null) {
                        throw new NotConnectedException();
                    }
                } catch (SecurityException ex) {
                    throw new PermissionDeniedException(ex);
                }
            } else {
                this.setCommandCallback(callback);
                if (!this.gatt.connect()) {
                    throw new RuntimeException("Unable to reconnect to device");
                }
            }
        });
    }

    @SuppressLint("MissingPermission")
    public Future<Void> disconnect() {
        SimpleFuture<Void> future = new SimpleFuture<>();
        synchronized (this) {
            // Teardown cannot wait behind an OS callback that may never arrive.
            // Acknowledgement means this GATT client has been closed, not that
            // all other apps have disconnected the device's shared radio link.
            if (this.pendingRetry != null) {
                this.pendingRetry.cancel(false);
                this.pendingRetry = null;
            }
            SimpleFuture<?> interrupted = this.activeFuture;
            this.activeFuture = null;
            this.commandCallback = null;
            this.executingCommand = false;
            if (interrupted != null) interrupted.wakeWithThrowable(new NotConnectedException());
            while (!this.commandQueue.isEmpty()) {
                this.commandQueue.remove().future.wakeWithThrowable(new NotConnectedException());
            }
            this.connected = false;
            this.closeNotificationStreams();
            try {
                if (this.gatt != null) {
                    try { this.gatt.disconnect(); }
                    finally {
                        this.gatt.close();
                        this.gatt = null;
                    }
                }
                future.wake(null);
            } catch (Throwable ex) {
                future.wakeWithThrowable(ex);
            }
        }
        return future;
    }

    public boolean isConnected() {
        return this.connected;
    }

    @SuppressLint("MissingPermission")
    public String getDeviceName() {
        return this.device.getName();
    }

    /**
     * Returns cached connection parameters as [interval, latency, timeout],
     * or null if not yet available. Interval is in 1.25ms units, timeout in 10ms units.
     */
    public synchronized int[] getConnectionParameters() {
        if (this.connectionInterval < 0) {
            return null;
        }
        return new int[] { this.connectionInterval, this.connectionLatency, this.supervisionTimeout };
    }

    /**
     * Request a connection priority change.
     * @param priority 0=BALANCED, 1=HIGH, 2=LOW_POWER
     */
    @SuppressLint("MissingPermission")
    public synchronized boolean requestConnectionPriority(int priority) {
        if (!this.connected || this.gatt == null) {
            throw new NotConnectedException();
        }
        return this.gatt.requestConnectionPriority(priority);
    }

    @SuppressLint("MissingPermission")
    public Future<Integer> requestMtu(int mtu) {
        SimpleFuture<Integer> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }
                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onMtuChanged(BluetoothGatt gatt, int mtu, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("MTU negotiation failed, status: " + status);
                                }
                                Peripheral.this.wakeCommand(future, mtu);
                            });
                        }
                    });
                    if (!this.gatt.requestMtu(mtu)) {
                        throw new RuntimeException("Unable to request MTU");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    public Future<byte[]> read(UUID serviceUuid, UUID uuid) {
        SimpleFuture<byte[]> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }

                    BluetoothGattCharacteristic characteristic = this.getCharacteristicByUuid(serviceUuid, uuid);
                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
                            onCharacteristicRead(gatt, characteristic, characteristic.getValue(), status);
                        }

                        @Override
                        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, byte[] value, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("Unable to read characteristic, status: " + status);
                                }

                                if (!characteristic.getUuid().equals(uuid) || !characteristic.getService().getUuid().equals(serviceUuid)) {
                                    throw new UnexpectedCharacteristicException();
                                }

                                Peripheral.this.wakeCommand(future, java.util.Arrays.copyOf(value, value.length));
                            });
                        }
                    });
                    if (!this.gatt.readCharacteristic(characteristic)) {
                        throw new RuntimeException("Unable to read characteristic");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    public Future<Void> write(UUID serviceUuid, UUID uuid, byte[] data, int writeType) {
        SimpleFuture<Void> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }

                    BluetoothGattCharacteristic characteristic = this.getCharacteristicByUuid(serviceUuid, uuid);
                    characteristic.setValue(data);
                    characteristic.setWriteType(writeType);
                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onCharacteristicWrite(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("Unable to write characteristic, status: " + status);
                                }

                                if (!characteristic.getUuid().equals(uuid) || !characteristic.getService().getUuid().equals(serviceUuid)) {
                                    throw new UnexpectedCharacteristicException();
                                }

                                Peripheral.this.wakeCommand(future, null);
                            });
                        }
                    });
                    if (!this.gatt.writeCharacteristic(characteristic)) {
                        throw new RuntimeException("Unable to write characteristic");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    public Future<List<BluetoothGattService>> discoverServices() {
        SimpleFuture<List<BluetoothGattService>> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }

                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onServicesDiscovered(BluetoothGatt gatt, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("Unable to discover services, status: " + status);
                                }

                                Peripheral.this.wakeCommand(future, gatt.getServices());
                            });
                        }
                    });
                    if (!this.gatt.discoverServices()) {
                        throw new RuntimeException("Unable to discover services");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    public Future<Void> setCharacteristicNotification(UUID serviceUuid, UUID uuid, boolean enable) {
        return setCharacteristicNotification(serviceUuid, uuid, enable, false);
    }

    public Future<Void> setCharacteristicNotification(UUID serviceUuid, UUID uuid, boolean enable, boolean nonstandardCccd) {
        SimpleFuture<Void> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }

                    BluetoothGattCharacteristic characteristic = this.getCharacteristicByUuid(serviceUuid, uuid);
                    // The caller explicitly selects local routing without a CCCD write.
                    if (nonstandardCccd && (characteristic.getProperties() & BluetoothGattCharacteristic.PROPERTY_NOTIFY) == 0) {
                        throw new IllegalArgumentException("Nonstandard CCCD policy requires a notify characteristic");
                    }
                    BluetoothGattDescriptor descriptor = nonstandardCccd ? null : this.getDescriptorByUuid(serviceUuid, uuid, CLIENT_CHARACTERISTIC_CONFIGURATION_DESCRIPTOR);
                    if (!this.gatt.setCharacteristicNotification(characteristic, enable)) {
                        throw new RuntimeException("Unable to set characteristic notification");
                    }

                    if (nonstandardCccd) {
                        this.wakeCommand(future, null);
                        return;
                    }

                    byte[] cccdValue;
                    if (!enable) {
                        cccdValue = BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE;
                    } else if ((characteristic.getProperties() & BluetoothGattCharacteristic.PROPERTY_INDICATE) != 0) {
                        cccdValue = BluetoothGattDescriptor.ENABLE_INDICATION_VALUE;
                    } else {
                        cccdValue = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE;
                    }
                    descriptor.setValue(cccdValue);
                    if (!this.gatt.writeDescriptor(descriptor)) {
                        throw new RuntimeException("Unable to write client characteristic configuration descriptor");
                    }

                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onDescriptorWrite(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("Unable to write client characteristic configuration descriptor, status: " + status);
                                }

                                if (!descriptor.getUuid().equals(CLIENT_CHARACTERISTIC_CONFIGURATION_DESCRIPTOR) || !descriptor.getCharacteristic().getUuid().equals(uuid) || !descriptor.getCharacteristic().getService().getUuid().equals(serviceUuid)) {
                                    throw new UnexpectedCharacteristicException();
                                }

                                Peripheral.this.wakeCommand(future, null);
                            });
                        }
                    });
                });
            });
        }
        return future;
    }

    public Stream<BluetoothGattCharacteristic> getNotifications() {
        QueueStream<BluetoothGattCharacteristic> stream = new QueueStream<>(1024);
        synchronized (this) {
            // openble owns exactly one notification stream per physical generation.
            this.closeNotificationStreams();
            this.notificationStreams.add(new WeakReference<>(stream));
        }
        return stream;
    }

    private void closeNotificationStreams() {
        for (WeakReference<QueueStream<BluetoothGattCharacteristic>> ref : this.notificationStreams) {
            QueueStream<BluetoothGattCharacteristic> stream = ref.get();
            if (stream != null) stream.close();
        }
        this.notificationStreams.clear();
    }

    @SuppressLint("MissingPermission")
    public Future<byte[]> readDescriptor(UUID serviceUuid, UUID characteristic, UUID uuid) {
        SimpleFuture<byte[]> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }

                    BluetoothGattDescriptor descriptor = this.getDescriptorByUuid(serviceUuid, characteristic, uuid);
                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onDescriptorRead(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
                            onDescriptorRead(gatt, descriptor, status, descriptor.getValue());
                        }

                        @Override
                        public void onDescriptorRead(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status, byte[] value) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("Unable to read descriptor, status: " + status);
                                }

                                if (!descriptor.getUuid().equals(uuid) || !descriptor.getCharacteristic().getUuid().equals(characteristic) || !descriptor.getCharacteristic().getService().getUuid().equals(serviceUuid)) {
                                    throw new UnexpectedCharacteristicException();
                                }

                                Peripheral.this.wakeCommand(future, java.util.Arrays.copyOf(value, value.length));
                            });
                        }
                    });
                    if (!this.gatt.readDescriptor(descriptor)) {
                        throw new RuntimeException("Unable to read descriptor");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    public Future<Void> writeDescriptor(UUID serviceUuid, UUID characteristic, UUID uuid, byte[] data) {
        SimpleFuture<Void> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }

                    BluetoothGattDescriptor descriptor = this.getDescriptorByUuid(serviceUuid, characteristic, uuid);
                    descriptor.setValue(data);
                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onDescriptorWrite(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("Unable to write descriptor, status: " + status);
                                }

                                if (!descriptor.getUuid().equals(uuid) || !descriptor.getCharacteristic().getUuid().equals(characteristic) || !descriptor.getCharacteristic().getService().getUuid().equals(serviceUuid)) {
                                    throw new UnexpectedCharacteristicException();
                                }

                                Peripheral.this.wakeCommand(future, null);
                            });
                        }
                    });
                    if (!this.gatt.writeDescriptor(descriptor)) {
                        throw new RuntimeException("Unable to write descriptor");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    public Future<Integer> readRemoteRssi() {
        SimpleFuture<Integer> future = new SimpleFuture<>();
        synchronized (this) {
            this.queueCommand(future, () -> {
                this.asyncWithFuture(future, () -> {
                    if (!this.connected) {
                        throw new NotConnectedException();
                    }
                    this.setCommandCallback(new CommandCallback(future) {
                        @Override
                        public void onReadRemoteRssi(BluetoothGatt gatt, int rssi, int status) {
                            Peripheral.this.asyncWithFuture(future, () -> {
                                if (status != BluetoothGatt.GATT_SUCCESS) {
                                    throw new RuntimeException("RSSI read failed, status: " + status);
                                }
                                Peripheral.this.wakeCommand(future, rssi);
                            });
                        }
                    });
                    if (!this.gatt.readRemoteRssi()) {
                        throw new RuntimeException("Unable to read remote RSSI");
                    }
                });
            });
        }
        return future;
    }

    @SuppressLint("MissingPermission")
    private BluetoothGattCharacteristic getCharacteristicByUuid(UUID serviceUuid, UUID uuid) {
        BluetoothGattService service = null;
        if (this.gatt != null) {
            for (BluetoothGattService candidate : this.gatt.getServices()) {
                if (candidate.getUuid().equals(serviceUuid)) {
                    if (service != null) throw new UnexpectedCharacteristicException();
                    service = candidate;
                }
            }
        }
        if (service == null) throw new NoSuchCharacteristicException();
        BluetoothGattCharacteristic match = null;
        for (BluetoothGattCharacteristic characteristic : service.getCharacteristics()) {
            if (characteristic.getUuid().equals(uuid)) {
                if (match != null) throw new UnexpectedCharacteristicException();
                match = characteristic;
            }
        }
        if (match == null) throw new NoSuchCharacteristicException();
        return match;
    }

    @SuppressLint("MissingPermission")
    private BluetoothGattDescriptor getDescriptorByUuid(UUID serviceUuid, UUID characteristicUuid, UUID uuid) {
        BluetoothGattCharacteristic characteristic = getCharacteristicByUuid(serviceUuid, characteristicUuid);
        BluetoothGattDescriptor match = null;
        for (BluetoothGattDescriptor descriptor : characteristic.getDescriptors()) {
            if (descriptor.getUuid().equals(uuid)) {
                if (match != null) throw new UnexpectedCharacteristicException();
                match = descriptor;
            }
        }
        if (match == null) throw new NoSuchCharacteristicException();
        return match;
    }

    private void queueCommand(SimpleFuture<?> future, Runnable callback) {
        QueuedCommand command = new QueuedCommand(future, callback);
        if (this.executingCommand) {
            this.commandQueue.add(command);
        } else {
            this.executingCommand = true;
            this.activeFuture = future;
            callback.run();
        }
    }

    private void setCommandCallback(CommandCallback callback) {
        assert this.commandCallback == null;
        this.commandCallback = callback;
    }

    private void runNextCommand() {
        assert this.executingCommand;
        this.commandCallback = null;
        this.activeFuture = null;
        if (this.commandQueue.isEmpty()) {
            this.executingCommand = false;
        } else {
            QueuedCommand command = this.commandQueue.remove();
            this.activeFuture = command.future;
            command.callback.run();
        }
    }

    private <T> void wakeCommand(SimpleFuture<T> future, T result) {
        if (this.activeFuture != future) return;
        future.wake(result);
        this.runNextCommand();
    }

    private <T> void asyncWithFuture(SimpleFuture<T> future, Runnable callback) {
        if (this.activeFuture != future) return;
        try {
            callback.run();
        } catch (Throwable ex) {
            future.wakeWithThrowable(ex);
            this.runNextCommand();
        }
    }

    // Every BluetoothGattCallback method below runs on the Binder thread: none of them may let
    // a Throwable escape, or the process crashes. dispatchToCommandCallback is the single
    // choke point that guarantees that for calls forwarded into a CommandCallback.
    private void dispatchToCommandCallback(String callbackName, Runnable dispatch) {
        try {
            dispatch.run();
        } catch (Throwable ex) {
            Log.e(TAG, "Unexpected exception dispatching " + callbackName, ex);
        }
    }

    private class Callback extends BluetoothGattCallback {
        @Override
        public void onConnectionStateChange(BluetoothGatt gatt, int status, int newState) {
            boolean suppressDisconnectNotification;
            boolean nowConnected;
            synchronized (Peripheral.this) {
                // connectGatt is always called (and its result assigned to Peripheral.this.gatt)
                // while holding this same lock, so a callback for a superseded/stale gatt (e.g.
                // a retry already moved on to a new connectGatt) can be safely ignored here.
                if (gatt != Peripheral.this.gatt) {
                    Log.w(TAG, "Ignoring onConnectionStateChange for stale gatt");
                    return;
                }
                switch (newState) {
                    case BluetoothGatt.STATE_CONNECTED:
                        Peripheral.this.connected = true;
                        break;
                    case BluetoothGatt.STATE_DISCONNECTED:
                        Peripheral.this.connected = false;
                        Peripheral.this.closeNotificationStreams();
                        break;
                }
                // Reset before dispatch; the command callback below sets it back to true if needed.
                Peripheral.this.suppressDisconnectNotification = false;
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onConnectionStateChange",
                            () -> Peripheral.this.commandCallback.onConnectionStateChange(gatt, status, newState));
                }
                // A remote loss with no command callback still owns a GATT object.
                // Close only this event's object, never a newly started generation.
                if (newState == BluetoothGatt.STATE_DISCONNECTED && Peripheral.this.gatt == gatt) {
                    try {
                        gatt.close();
                        Peripheral.this.gatt = null;
                    } catch (Throwable ex) {
                        // Keep ownership for the worker's disconnect/recovery path.
                        // A failed local close must not suppress the remote-loss event.
                        Log.e(TAG, "Unable to close remotely disconnected GATT", ex);
                    }
                }
                suppressDisconnectNotification = Peripheral.this.suppressDisconnectNotification;
                // A failed connect closes the gatt and clears `connected` even if the state was CONNECTED.
                nowConnected = Peripheral.this.connected;
            }
            switch (newState) {
                case BluetoothGatt.STATE_CONNECTED:
                    if (nowConnected) {
                        Peripheral.this.dispatchToCommandCallback("adapterConnected",
                                () -> Peripheral.this.adapter.onConnectionStateChanged(Peripheral.this.device.getAddress(), true));
                    }
                    break;
                case BluetoothGatt.STATE_DISCONNECTED:
                    if (!suppressDisconnectNotification) {
                        Peripheral.this.dispatchToCommandCallback("adapterDisconnected",
                                () -> Peripheral.this.adapter.onConnectionStateChanged(Peripheral.this.device.getAddress(), false));
                    }
                    break;
            }
        }

        @Override
        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
            synchronized (Peripheral.this) {
                // A closed GATT may still deliver Binder callbacks after reconnect.
                // Never route those callbacks into the current generation.
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onCharacteristicRead",
                            () -> Peripheral.this.commandCallback.onCharacteristicRead(gatt, characteristic, status));
                }
            }
        }

        @Override
        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, byte[] value, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onCharacteristicRead",
                            () -> Peripheral.this.commandCallback.onCharacteristicRead(gatt, characteristic, value, status));
                }
            }
        }

        @Override
        public void onCharacteristicWrite(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onCharacteristicWrite",
                            () -> Peripheral.this.commandCallback.onCharacteristicWrite(gatt, characteristic, status));
                }
            }
        }

        @Override
        public void onServicesDiscovered(BluetoothGatt gatt, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onServicesDiscovered",
                            () -> Peripheral.this.commandCallback.onServicesDiscovered(gatt, status));
                }
            }
        }

        @Override
        public void onCharacteristicChanged(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                onCharacteristicChanged(gatt, characteristic, characteristic.getValue());
            }
        }

        @Override
        public void onCharacteristicChanged(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, byte[] value) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                BluetoothGattCharacteristic characteristic2 = new BluetoothGattCharacteristic(characteristic.getUuid(), characteristic.getProperties(), characteristic.getPermissions());
                characteristic2.setValue(java.util.Arrays.copyOf(value, value.length));
                BluetoothGattService sourceService = characteristic.getService();
                BluetoothGattService serviceCopy = new BluetoothGattService(sourceService.getUuid(), sourceService.getType());
                serviceCopy.addCharacteristic(characteristic2);
                for (WeakReference<QueueStream<BluetoothGattCharacteristic>> ref : Peripheral.this.notificationStreams) {
                    QueueStream<BluetoothGattCharacteristic> stream = ref.get();
                    if (stream != null) {
                        stream.add(characteristic2);
                    }
                }
            }
        }

        @Override
        public void onDescriptorRead(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onDescriptorRead",
                            () -> Peripheral.this.commandCallback.onDescriptorRead(gatt, descriptor, status));
                }
            }
        }

        @Override
        public void onDescriptorRead(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status, byte[] value) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onDescriptorRead",
                            () -> Peripheral.this.commandCallback.onDescriptorRead(gatt, descriptor, status, value));
                }
            }
        }

        @Override
        public void onDescriptorWrite(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onDescriptorWrite",
                            () -> Peripheral.this.commandCallback.onDescriptorWrite(gatt, descriptor, status));
                }
            }
        }

        @Override
        public void onMtuChanged(BluetoothGatt gatt, int mtu, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onMtuChanged",
                            () -> Peripheral.this.commandCallback.onMtuChanged(gatt, mtu, status));
                }
            }
        }

        @Override
        public void onReadRemoteRssi(BluetoothGatt gatt, int rssi, int status) {
            synchronized (Peripheral.this) {
                if (gatt != Peripheral.this.gatt) {
                    return;
                }
                if (Peripheral.this.commandCallback != null) {
                    Peripheral.this.dispatchToCommandCallback("onReadRemoteRssi",
                            () -> Peripheral.this.commandCallback.onReadRemoteRssi(gatt, rssi, status));
                }
            }
        }

        // Note: onConnectionUpdated is a hidden API in BluetoothGattCallback — no @Override.
        public void onConnectionUpdated(BluetoothGatt gatt, int interval, int latency, int timeout, int status) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                synchronized (Peripheral.this) {
                    if (gatt != Peripheral.this.gatt) {
                        return;
                    }
                    Peripheral.this.connectionInterval = interval;
                    Peripheral.this.connectionLatency = latency;
                    Peripheral.this.supervisionTimeout = timeout;
                }
            }
        }
    }

    private abstract class CommandCallback extends BluetoothGattCallback {
        private final SimpleFuture<?> future;

        CommandCallback(SimpleFuture<?> future) {
            this.future = future;
        }

        // Default: a disconnect during any command this base isn't overridden for (i.e. every
        // command except connect/disconnect themselves, which override this) fails that
        // command's future with NotConnectedException and advances the queue, instead of relying
        // on each subclass to remember to override this. connect/disconnect have their own
        // onConnectionStateChange semantics and override this method entirely.
        @Override
        public void onConnectionStateChange(BluetoothGatt gatt, int status, int newState) {
            if (newState == BluetoothGatt.STATE_DISCONNECTED) {
                Peripheral.this.asyncWithFuture(this.future, () -> {
                    if (Peripheral.this.gatt != null) {
                        Peripheral.this.gatt.close();
                        Peripheral.this.gatt = null;
                    }
                    throw new NotConnectedException();
                });
            }
        }

        // The following are stray/unsolicited callbacks for a command that didn't ask for them
        // (e.g. an onMtuChanged arriving while a read() is in flight). They're logged and
        // ignored rather than failing the in-flight command or throwing, since they aren't
        // evidence that the in-flight command itself failed.
        @Override
        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
            Log.w(TAG, "Unexpected onCharacteristicRead callback");
        }

        @Override
        public void onCharacteristicWrite(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {
            Log.w(TAG, "Unexpected onCharacteristicWrite callback");
        }

        @Override
        public void onDescriptorRead(BluetoothGatt gatt, BluetoothGattDescriptor descriptor,
                                     int status) {
            Log.w(TAG, "Unexpected onDescriptorRead callback");
        }

        @Override
        public void onServicesDiscovered(BluetoothGatt gatt, int status) {
            Log.w(TAG, "Unexpected onServicesDiscovered callback");
        }

        @Override
        public void onDescriptorWrite(BluetoothGatt gatt, BluetoothGattDescriptor descriptor, int status) {
            Log.w(TAG, "Unexpected onDescriptorWrite callback");
        }

        @Override
        public void onMtuChanged(BluetoothGatt gatt, int mtu, int status) {
            Log.w(TAG, "Unexpected onMtuChanged callback");
        }

        @Override
        public void onReadRemoteRssi(BluetoothGatt gatt, int rssi, int status) {
            Log.w(TAG, "Unexpected onReadRemoteRssi callback");
        }
    }
}
