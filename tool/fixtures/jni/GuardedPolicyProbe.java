package io.openble.test;

/** Host-JVM test fixture; never included in the Android consumer artifact. */
public final class GuardedPolicyProbe {
    private GuardedPolicyProbe() {}
    public static native int fail();
}
