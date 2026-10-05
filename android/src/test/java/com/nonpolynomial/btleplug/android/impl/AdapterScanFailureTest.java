package com.nonpolynomial.btleplug.android.impl;

import android.bluetooth.le.ScanCallback;
import java.lang.reflect.*;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;

public class AdapterScanFailureTest {
    static class RecordingAdapter extends Adapter {
        final List<String> failures = new ArrayList<>();
        @Override protected void publishScanFailure(long generation, int code) {
            failures.add(generation + ":" + code);
        }
    }
    static ScanCallback attempt(Adapter owner, long generation) throws Exception {
        Class<?> type = Class.forName(Adapter.class.getName() + "$Callback");
        Constructor<?> constructor = type.getDeclaredConstructor(Adapter.class, long.class);
        constructor.setAccessible(true);
        ScanCallback callback = (ScanCallback) constructor.newInstance(owner, generation);
        Field current = Adapter.class.getDeclaredField("callback"); current.setAccessible(true); current.set(owner, callback);
        Field counter = Adapter.class.getDeclaredField("generation"); counter.setAccessible(true); counter.setLong(owner, generation);
        return callback;
    }
    @Test public void failureIsReportedOnceAndCachedWithoutSuppressingItsCode() throws Exception {
        for (int code : new int[]{1,2,3,4,5,6,99}) {
            RecordingAdapter owner = new RecordingAdapter();
            ScanCallback callback = attempt(owner, 1);
            callback.onScanFailed(code); callback.onScanFailed(3);
            assertEquals(List.of("1:" + code), owner.failures);
            assertArrayEquals(new long[]{1,code}, owner.getScanState());
            // An advertisement after failure must not call unregistered JNI.
            callback.onScanResult(1, null);
        }
    }
    @Test public void oldCallbackCannotFailOrPublishIntoNextAttempt() throws Exception {
        RecordingAdapter owner = new RecordingAdapter();
        for (long generation = 1; generation <= 100; generation++) {
            ScanCallback old = attempt(owner, generation * 2 - 1);
            ScanCallback next = attempt(owner, generation * 2);
            old.onScanFailed(3); old.onScanResult(1, null);
            assertArrayEquals(new long[]{generation * 2,0}, owner.getScanState());
            next.onScanFailed(6);
            assertEquals(generation, owner.failures.size());
            assertEquals((generation * 2) + ":6", owner.failures.get(owner.failures.size()-1));
        }
    }
    @Test public void retiredCallbackCannotPublishAfterStop() throws Exception {
        RecordingAdapter owner = new RecordingAdapter();
        ScanCallback callback = attempt(owner, 1);
        Field current = Adapter.class.getDeclaredField("callback");
        current.setAccessible(true); current.set(owner, null);
        callback.onScanFailed(3); callback.onScanResult(1, null);
        assertTrue(owner.failures.isEmpty());
        assertArrayEquals(new long[]{1,0}, owner.getScanState());
    }
}
