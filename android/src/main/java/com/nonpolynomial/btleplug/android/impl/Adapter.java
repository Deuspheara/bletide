package com.nonpolynomial.btleplug.android.impl;

import android.annotation.SuppressLint;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothManager;
import android.bluetooth.le.BluetoothLeScanner;
import android.bluetooth.le.ScanCallback;
import android.bluetooth.le.ScanFilter.Builder;
import android.bluetooth.le.ScanResult;
import android.bluetooth.le.ScanSettings;
import android.os.Build;
import android.os.ParcelUuid;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused") // Native code uses this class.
class Adapter {
    private long handle;
    private Callback callback;
    private long generation;

    public Adapter() {}

    @SuppressLint("MissingPermission")
    public synchronized void startScan(ScanFilter filter) {
        BluetoothAdapter bluetoothAdapter = BluetoothAdapter.getDefaultAdapter();
        if (bluetoothAdapter == null) {
          throw new NoBluetoothAdapterException();
        }

        ArrayList<android.bluetooth.le.ScanFilter> filters = null;
        String[] uuids = filter.getUuids();
        if (uuids.length > 0) {
            filters = new ArrayList<>();
            for (String uuid : uuids) {
                filters.add(new Builder().setServiceUuid(ParcelUuid.fromString(uuid)).build());
            }
        }
        ScanSettings settings;
        if (Build.VERSION.SDK_INT >= 26) {
            settings = new ScanSettings.Builder()
                    .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
                    .setLegacy(false)
                    .build();
        } else {
            settings = new ScanSettings.Builder()
                    .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
                    .build();
        }
        BluetoothLeScanner scanner = bluetoothAdapter.getBluetoothLeScanner();
        if (scanner == null) {
          throw new NoBluetoothAdapterException();
        }
        if (this.callback != null) {
            throw new IllegalStateException("Previous scan requires cleanup");
        }
        if (generation == Long.MAX_VALUE) {
            throw new IllegalStateException("Scan generation exhausted");
        }
        this.callback = new Callback(++generation);
        scanner.startScan(filters, settings, this.callback);
    }

    @SuppressLint("MissingPermission")
    public synchronized void stopScan() {
        if (this.callback == null) return;
        BluetoothAdapter bluetoothAdapter = BluetoothAdapter.getDefaultAdapter();
        if (bluetoothAdapter != null) {
            BluetoothLeScanner scanner = bluetoothAdapter.getBluetoothLeScanner();
            if (scanner != null) {
                scanner.stopScan(this.callback);
            }
        }
        this.callback = null;
    }

    public synchronized long[] getScanState() {
        return new long[]{generation, callback == null ? 0 : callback.failure};
    }

    private native void reportScanFailed(long generation, int errorCode);

    protected void publishScanFailure(long generation, int errorCode) {
        reportScanFailed(generation, errorCode);
    }

    private native void reportScanResult(ScanResult result);

    public native void onConnectionStateChanged(String address, boolean connected);

    private class Callback extends ScanCallback {
        private final long generation;
        private int failure;
        Callback(long generation) { this.generation = generation; }

        @Override
        public void onScanResult(int callbackType, ScanResult result) {
            synchronized (Adapter.this) {
                if (Adapter.this.callback != this || failure != 0) return;
                Adapter.this.reportScanResult(result);
            }
        }

        @Override
        public void onScanFailed(int errorCode) {
            synchronized (Adapter.this) {
                if (Adapter.this.callback != this || failure != 0) return;
                failure = errorCode;
                Adapter.this.publishScanFailure(generation, errorCode);
            }
        }
    }
}
