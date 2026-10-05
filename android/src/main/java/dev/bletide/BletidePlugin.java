package dev.bletide;

import android.Manifest;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothManager;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageManager;
import android.os.Build;
import io.flutter.embedding.engine.plugins.FlutterPlugin;
import io.flutter.embedding.engine.plugins.activity.ActivityAware;
import io.flutter.embedding.engine.plugins.activity.ActivityPluginBinding;
import io.flutter.plugin.common.PluginRegistry;

/** JNI/class-loader bootstrap and adapter state only. BLE operations use Rust FFI. */
public final class BletidePlugin implements FlutterPlugin, ActivityAware,
        PluginRegistry.RequestPermissionsResultListener {
    static { System.loadLibrary("bletide"); }
    private static synchronized void bootstrap() { nativeInitialize(); }
    private static native void nativeInitialize();
    private static native void nativeAdapterState(int state);
    private Context context;
    private ActivityPluginBinding activity;
    private final BroadcastReceiver receiver = new BroadcastReceiver() {
        @Override public void onReceive(Context context, Intent intent) { updateState(); }
    };
    @Override public void onAttachedToEngine(FlutterPluginBinding binding) {
        bootstrap();
        context = binding.getApplicationContext();
        IntentFilter filter = new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED);
        if (Build.VERSION.SDK_INT >= 33) {
            context.registerReceiver(receiver, filter, Context.RECEIVER_EXPORTED);
        } else {
            context.registerReceiver(receiver, filter);
        }
        updateState();
    }
    private void updateState() {
        if (context == null) return;
        if (Build.VERSION.SDK_INT >= 31 &&
                (context.checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED ||
                 context.checkSelfPermission(Manifest.permission.BLUETOOTH_SCAN) != PackageManager.PERMISSION_GRANTED)) {
            nativeAdapterState(3); return;
        }
        if (Build.VERSION.SDK_INT < 31 &&
                context.checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            nativeAdapterState(3); return;
        }
        try {
            BluetoothManager manager = (BluetoothManager) context.getSystemService(Context.BLUETOOTH_SERVICE);
            BluetoothAdapter adapter = manager == null ? null : manager.getAdapter();
            nativeAdapterState(adapter == null ? 1 : adapter.isEnabled() ? 4 : 2);
        } catch (SecurityException exception) { nativeAdapterState(3); }
    }
    @Override public void onDetachedFromEngine(FlutterPluginBinding binding) {
        detachActivity();
        if (context != null) { context.unregisterReceiver(receiver); context = null; }
    }
    @Override public void onAttachedToActivity(ActivityPluginBinding binding) {
        activity = binding; binding.addRequestPermissionsResultListener(this); updateState();
    }
    private void detachActivity() {
        if (activity != null) { activity.removeRequestPermissionsResultListener(this); activity = null; }
    }
    @Override public void onDetachedFromActivity() { detachActivity(); }
    @Override public void onDetachedFromActivityForConfigChanges() { detachActivity(); }
    @Override public void onReattachedToActivityForConfigChanges(ActivityPluginBinding binding) { onAttachedToActivity(binding); }
    @Override public boolean onRequestPermissionsResult(int request, String[] permissions, int[] grants) {
        updateState(); return false;
    }
}
