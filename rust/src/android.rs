//! Android class-loader initialization and adapter-state bridge, never BLE calls.
use crate::codec::Error;
use jni::{EnvUnowned, objects::JClass, sys::jint};
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};
static INITIALIZED: AtomicBool = AtomicBool::new(false);
static STATE: OnceLock<crate::adapter_state::AdapterState> = OnceLock::new();
fn state() -> &'static crate::adapter_state::AdapterState {
    STATE.get_or_init(crate::adapter_state::AdapterState::default)
}
pub(crate) fn adapter_state() -> Result<u8, Error> {
    if !INITIALIZED.load(Ordering::Acquire) {
        return Err(Error::new(18, "Android JNI bootstrap is missing or failed"));
    }
    Ok(state().value())
}
pub(crate) fn state_events() -> (
    u8,
    impl futures_util::Stream<Item = Result<Vec<u8>, Error>> + Send,
) {
    use futures_util::StreamExt;
    let (initial, events) = state().subscribe();
    (
        initial,
        events.map(|value| Ok(crate::codec::event(3, 0, Ok(vec![value])))),
    )
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_bletide_BletidePlugin_nativeInitialize<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) {
    env.with_env(|env| -> btleplug::Result<()> {
        btleplug::platform::init(env)?;
        INITIALIZED.store(true, Ordering::Release);
        Ok(())
    })
    .resolve::<crate::android_error_policy::GuardedRuntimeError>();
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_bletide_BletidePlugin_nativeAdapterState<'local>(
    _env: EnvUnowned<'local>,
    _class: JClass<'local>,
    value: jint,
) {
    if crate::callback_boundary::invoke(
        || {
            if (1..=4).contains(&value) {
                state().update(value as u8, || {
                    crate::shared_scan::process_scanner().invalidate(value as u8)
                });
            }
            Ok::<(), ()>(())
        },
        || (),
    )
    .is_err()
    {
        INITIALIZED.store(false, Ordering::Release);
    }
}
