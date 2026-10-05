//! JNI error mapping with owned panic-payload disposal.
use jni::{
    Env,
    errors::{ErrorPolicy, ThrowRuntimeExAndDefault},
};
use std::any::Any;

use super::callback_boundary;

pub(crate) struct GuardedRuntimeError;

fn dispose_payload(payload: Box<dyn Any + Send + 'static>) {
    let _ = callback_boundary::invoke(
        || {
            drop(payload);
            Ok::<(), ()>(())
        },
        || (),
    );
}

impl<T: Default, E: std::error::Error> ErrorPolicy<T, E> for GuardedRuntimeError {
    type Captures<'local: 'method, 'method> = ();

    fn on_error<'local: 'method, 'method>(
        env: &mut Env<'local>,
        captures: &mut (),
        error: E,
    ) -> jni::errors::Result<T> {
        <ThrowRuntimeExAndDefault as ErrorPolicy<T, E>>::on_error(env, captures, error)
    }

    fn on_panic<'local: 'method, 'method>(
        env: &mut Env<'local>,
        _captures: &mut (),
        payload: Box<dyn Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        if env.exception_check() {
            // Preserve the original Java failure while still releasing Rust's
            // panic payload. No payload ownership is transferred to Java.
            dispose_payload(payload);
            return Ok(T::default());
        }
        let message = if let Some(value) = payload.downcast_ref::<&str>() {
            (*value).to_owned()
        } else if let Some(value) = payload.downcast_ref::<String>() {
            value.clone()
        } else {
            "non-string panic payload".to_owned()
        };
        dispose_payload(payload);
        // JNI reports JavaException after a successful throw; it stays pending
        // for Java, just as in the original ThrowRuntimeExAndDefault policy.
        let _ = env.throw(format!("Rust panic: {message}"));
        Ok(T::default())
    }

    fn on_internal_panic<'local: 'method, 'method>(
        _captures: &mut (),
        payload: Box<dyn Any + Send + 'static>,
    ) -> T
    where
        T: Default,
    {
        dispose_payload(payload);
        T::default()
    }
}
