#ifndef OPENBLE_H
#define OPENBLE_H
#include <stdbool.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
// ABI v2; positive handles/request IDs, negative portable error code, zero invalid/no-op.
int64_t bletide_abi_version(void);
// Forward declaration keeps this header usable without importing the Dart SDK.
struct _Dart_CObject;
typedef bool (*bletide_post_c_object)(int64_t port, struct _Dart_CObject *message);
// post_c_object is NativeApi.postCObject. The Dart VM must remain running.
int64_t bletide_open(int64_t port, bletide_post_c_object post_c_object);
// length must be <= 1 MiB. A nonzero length requires that many readable bytes
// for this synchronous call; payload may be null when length is zero.
// Copies input before returning and retains no pointer. Completion is copied
// to the native port; the caller continues to own its input buffer.
int64_t bletide_command(uint64_t engine, uint32_t operation,
    const uint8_t *payload, uint32_t length, uint32_t timeout_ms);
int64_t bletide_cancel(uint64_t engine, uint64_t request);
// Returns a close request ID; keep the VM running until its port acknowledgement.
// Repeated close is idempotent.
int64_t bletide_close(uint64_t engine);
#ifdef __cplusplus
}
#endif
#endif
