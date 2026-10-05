use jni::{
    Env,
    errors::Result,
    objects::{JByteArray, JObject},
};

pub fn slice_to_byte_array<'local>(
    env: &mut Env<'local>,
    slice: &[u8],
) -> Result<JByteArray<'local>> {
    env.byte_array_from_slice(slice)
}

pub fn byte_array_to_vec(env: &Env, array: &JByteArray) -> Result<Vec<u8>> {
    // The pinned JNI helper initializes its output before exposing a byte slice.
    env.convert_byte_array(array)
}

/// Check a generic Java value before copying its byte array into Rust ownership.
pub(crate) fn object_to_byte_vec(env: &Env, value: JObject) -> Result<Vec<u8>> {
    let array = env.cast_local::<JByteArray>(value)?;
    byte_array_to_vec(env, &array)
}

#[cfg(test)]
mod test {
    use super::super::test_utils;

    #[test]
    fn binary_roundtrips_cover_empty_all_bits_and_repeated_buffers() {
        for cycle in 0..100u8 {
            for input in [
                Vec::new(),
                (0..=255u8).collect(),
                (0..4096)
                    .map(|value| (value as u8).wrapping_add(cycle))
                    .collect(),
            ] {
                test_utils::with_env(|env| {
                    let array = super::slice_to_byte_array(env, &input)?;
                    assert_eq!(super::byte_array_to_vec(env, &array)?, input);
                    let object = env.new_local_ref(&array)?;
                    assert_eq!(super::object_to_byte_vec(env, object.into())?, input);
                    assert!(!env.exception_check());
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn null_byte_array_returns_error_without_retained_java_exception() {
        test_utils::with_env(|env| {
            let array = jni::objects::JByteArray::null();
            assert!(super::byte_array_to_vec(env, &array).is_err());
            assert!(super::object_to_byte_vec(env, jni::objects::JObject::null()).is_err());
            assert!(!env.exception_check());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn generic_java_values_reject_non_byte_arrays_without_pending_exception() {
        test_utils::with_env(|env| {
            let wrong = env.new_string("not a byte array")?;
            assert!(matches!(
                super::object_to_byte_vec(env, wrong.into()),
                Err(jni::errors::Error::WrongObjectType)
            ));
            let wrong = env.new_int_array(3)?;
            assert!(matches!(
                super::object_to_byte_vec(env, wrong.into()),
                Err(jni::errors::Error::WrongObjectType)
            ));
            assert!(!env.exception_check());
            let valid = super::slice_to_byte_array(env, &[0, 128, 255])?;
            assert_eq!(super::object_to_byte_vec(env, valid.into())?, [0, 128, 255]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_slice_to_byte_array() {
        test_utils::with_env(|env| {
            let obj = super::slice_to_byte_array(env, &[1, 2, 3, 4, 5]).unwrap();
            assert_eq!(obj.len(env).unwrap(), 5);

            let mut bytes = [0i8; 5];
            obj.get_region(env, 0, &mut bytes).unwrap();
            assert_eq!(bytes, [1, 2, 3, 4, 5]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_byte_array_to_vec() {
        test_utils::with_env(|env| {
            let obj = env.new_byte_array(5).unwrap();
            obj.set_region(env, 0, &[1, 2, 3, 4, 5]).unwrap();

            let vec = super::byte_array_to_vec(env, &obj).unwrap();
            assert_eq!(vec, vec![1, 2, 3, 4, 5]);
            Ok(())
        })
        .unwrap();
    }
}
