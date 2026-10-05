use super::error_policy::*;
use crate::droidplug::jni_utils::test_utils;
use jni::errors::ErrorPolicy;
use std::any::Any;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn exception_message(env: &mut jni::Env<'_>) -> jni::errors::Result<String> {
    let exception = env.exception_occurred().expect("Expected Java exception");
    env.exception_clear();
    let message = env
        .call_method(
            &exception,
            jni::jni_str!("getMessage"),
            jni::jni_sig!("()Ljava/lang/String;"),
            &[],
        )?
        .l()?;
    Ok(env
        .cast_local::<jni::objects::JString>(message)?
        .to_string())
}

struct Secondary(Arc<AtomicUsize>);
impl Drop for Secondary {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
struct Primary(Arc<AtomicUsize>);
impl Drop for Primary {
    fn drop(&mut self) {
        std::panic::panic_any(Secondary(self.0.clone()));
    }
}

#[test]
fn jni_panic_policy_releases_secondary_owner_and_throws() {
    test_utils::with_env(|env| {
        let owner = Arc::new(AtomicUsize::new(0));
        <GuardedRuntimeError as ErrorPolicy<(), std::io::Error>>::on_panic(
            env,
            &mut (),
            Box::new(Primary(owner.clone())),
        )?;
        assert!(env.exception_check());
        env.exception_clear();
        assert_eq!(Arc::strong_count(&owner), 1);
        assert_eq!(owner.load(Ordering::Relaxed), 1);
        Ok(())
    })
    .unwrap();
}

#[test]
fn jni_internal_panic_policy_releases_secondary_owner() {
    let owner = Arc::new(AtomicUsize::new(0));
    <GuardedRuntimeError as ErrorPolicy<(), std::io::Error>>::on_internal_panic(
        &mut (),
        Box::new(Primary(owner.clone())),
    );
    assert_eq!(Arc::strong_count(&owner), 1);
    assert_eq!(owner.load(Ordering::Relaxed), 1);
}

#[test]
fn jni_policy_preserves_original_error_and_string_panic_message() {
    test_utils::with_env(|env| {
        <GuardedRuntimeError as ErrorPolicy<(), std::io::Error>>::on_error(
            env,
            &mut (),
            std::io::Error::other("original JNI error"),
        )?;
        let owner = Arc::new(AtomicUsize::new(0));
        <GuardedRuntimeError as ErrorPolicy<(), std::io::Error>>::on_panic(
            env,
            &mut (),
            Box::new(Primary(owner.clone())),
        )?;
        assert_eq!(exception_message(env)?, "Rust error: original JNI error");
        assert_eq!(Arc::strong_count(&owner), 1);
        assert_eq!(owner.load(Ordering::Relaxed), 1);
        for (payload, expected) in [
            (
                Box::new("literal cause") as Box<dyn Any + Send>,
                "Rust panic: literal cause",
            ),
            (
                Box::new("owned cause".to_owned()) as Box<dyn Any + Send>,
                "Rust panic: owned cause",
            ),
        ] {
            <GuardedRuntimeError as ErrorPolicy<(), std::io::Error>>::on_panic(
                env,
                &mut (),
                payload,
            )?;
            assert_eq!(exception_message(env)?, expected);
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn actual_foreign_entry_resolver_releases_owner_and_sets_java_exception() {
    extern "system" fn invoke_policy(
        mut env: jni::EnvUnowned<'_>,
        owner: &Arc<AtomicUsize>,
    ) -> i32 {
        env.with_env(|_| -> jni::errors::Result<i32> {
            std::panic::panic_any(Primary(owner.clone()));
        })
        .resolve::<GuardedRuntimeError>()
    }
    test_utils::with_env(|env| {
        let owner = Arc::new(AtomicUsize::new(0));
        // This Env pointer is valid on the currently attached JVM test thread for
        // the entire synchronous foreign-entry call; no reference escapes it.
        let unowned = unsafe { jni::EnvUnowned::from_raw(env.get_raw()) };
        assert_eq!(invoke_policy(unowned, &owner), 0);
        assert!(env.exception_check());
        env.exception_clear();
        assert_eq!(Arc::strong_count(&owner), 1);
        assert_eq!(owner.load(Ordering::Relaxed), 1);
        Ok(())
    })
    .unwrap();
}

#[test]
fn actual_resolver_internal_fallback_releases_secondary_owner() {
    #[derive(Debug)]
    struct FormattingError(Arc<AtomicUsize>);
    impl std::fmt::Display for FormattingError {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::panic::panic_any(Primary(self.0.clone()));
        }
    }
    impl std::error::Error for FormattingError {}
    impl From<jni::errors::Error> for FormattingError {
        fn from(error: jni::errors::Error) -> Self {
            panic!("Unexpected JNI setup failure: {error}");
        }
    }

    test_utils::with_env(|env| {
        let owner = Arc::new(AtomicUsize::new(0));
        // The synchronous resolver runs on the attached test thread. Its error
        // formatter panics before throwing, exercising on_internal_panic.
        let mut unowned = unsafe { jni::EnvUnowned::from_raw(env.get_raw()) };
        let result = unowned
            .with_env(|_| -> Result<i32, FormattingError> { Err(FormattingError(owner.clone())) })
            .resolve::<GuardedRuntimeError>();
        assert_eq!(result, 0);
        assert!(!env.exception_check());
        assert_eq!(Arc::strong_count(&owner), 1);
        assert_eq!(owner.load(Ordering::Relaxed), 1);
        Ok(())
    })
    .unwrap();
}

#[test]
fn java_dispatch_through_generated_wrapper_releases_secondary_owner() {
    thread_local! {
        static OWNER: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    }
    fn fail<'local>(
        _: &mut jni::Env<'local>,
        _: jni::objects::JClass<'local>,
    ) -> jni::errors::Result<i32> {
        OWNER.with(|owner| std::panic::panic_any(Primary(owner.clone())))
    }
    test_utils::with_env(|env| {
        let class = env.find_class(jni::jni_str!("io/openble/test/GuardedPolicyProbe"))?;
        // The macro creates the type-checked native wrapper; its signature
        // exactly matches the dedicated host-only Java class's static method.
        unsafe {
            env.register_native_methods(
                &class,
                &[jni::native_method! {
                    name = "fail",
                    sig = () -> i32,
                    static = true,
                    fn = fail,
                    error_policy = GuardedRuntimeError,
                }],
            )?;
        }
        let result =
            env.call_static_method(&class, jni::jni_str!("fail"), jni::jni_sig!("()I"), &[]);
        assert!(matches!(result, Err(jni::errors::Error::JavaException)));
        assert_eq!(
            exception_message(env)?,
            "Rust panic: non-string panic payload"
        );
        OWNER.with(|owner| {
            assert_eq!(Arc::strong_count(owner), 1);
            assert_eq!(owner.load(Ordering::Relaxed), 1);
        });
        Ok(())
    })
    .unwrap();
}
