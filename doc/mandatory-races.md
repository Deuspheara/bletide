# Mandatory race evidence crosswalk

This maps each race named in the original implementation objective to inspected
assertions. Evidence is deterministic library behavior, not physical platform
verification. A matching test name alone is insufficient. `Covered` below means
the stated narrow assertion is present, not that every OS realization is proven.
Host and Linux tests pass 161 Dart/FFI cases and 71 Rust cases at this review.

Abbreviations: **D** = `test/backend_contract_test.dart`, **N** =
`test/native_backend_test.dart`, **A** = `test/native_abi_test.dart`, **G** =
`test/support/gatt_contract.dart`, **R** = Rust source test modules.

| Required race | Inspected test/assertion | Evidence and remaining gap |
|---|---|---|
| Disconnect during connect | D `disconnect during connect cancels once and ignores late completion` | Covered: cancelled result, late platform failure, empty fake pending registry |
| Disconnect during read | D parameterized `disconnect invalidates running read and queued work` | Covered: both futures disconnected; pending registry empty; repeated disconnect |
| Disconnect during write | Same D loop; G `disconnect cancels an issued write and rejects queued work` | Covered: issued/queued failures, fresh connection usable, old object rejected |
| Disconnect during discovery | Same D loop, `discover` branch | Covered: running discovery and queued RSSI fail, state disconnected |
| Timeout while operation completes | N terminal-order loop including `completionAtDeadlineFirst`/`timeoutAtDeadlineFirst` | Covered with virtual clock and controlled native transport: both timer registration orders at identical one-second deadlines, one terminal result, FIFO progress, no timers or pending transport requests |
| Cancel while operation completes | N same loop, `completed`, `cancelledBeforeDelivery`, `cancelled` | Covered: both orders and completed OS future before public delivery; one result and FIFO progress |
| Remote disconnect while request queued | D `remote disconnect cancels queued and running requests` | Covered: queued write fails and pending registry clears |
| Remote disconnect while request running | Same D case | Covered: issued read fails and pending registry clears |
| Late read completion from old generation | N `old read result after reconnect cannot affect replacement; error=false/true` | Covered with controlled native transport: old future disconnected once, late success/error after replacement, no stale diagnostics, fresh generation read succeeds |
| Late write completion from old generation | N `old write result after reconnect cannot affect replacement; error=false/true` | Covered with controlled native transport: old write disconnected once, both late result types after replacement, fresh read succeeds and old object rejected |
| Late error from old generation | N `late malformed values and errors cannot close or report into a fresh generation` | Covered for notification/disconnect event kinds and pending read/write errors after reconnect: no stale error/diagnostic, replacement operation succeeds |
| Late disconnect from old generation | N `old generations cannot notify or disconnect a replacement connection` | Covered: old disconnect then replacement notification and operation succeed |
| Late notification from old generation | Same N case; D fresh-generation case; G buffered notification cases | Covered: only new generation value delivered; paused old data discarded after close |
| Unsubscribe while notification arrives | D `final notification cancellation waits for stop acknowledgement` | Covered: injected value while stop pending omitted; cancellation waits; subscription empty |
| Unsubscribe while subscribe starting | D `unsubscribe during subscribe startup compensates after startup finishes` | Covered: acknowledged setup compensated by unsubscribe; FIFO barrier confirms no subscription |
| Two consumers subscribe simultaneously | D `two notification consumers share startup; only last owner unsubscribes` | Covered: one physical setup and live second listener |
| One notification consumer leaves | Same D case; G owner-sharing case | Covered: remaining owner receives data, no physical stop until final owner |
| Last notification consumer leaves | Same D case; final cancellation acknowledgement case | Covered: acknowledged physical unsubscribe and no subscription |
| Scan cancelled during startup | D `scan cancellation during platform startup still stops the physical scan` | Covered: startup completed after cancel intent; stop acknowledged; pending empty |
| Last scan listener leaves while advertisement arrives | D `final scan owner cancellation excludes racing advertisements; queued=true/false` | Covered in fake backend: queued value before cancellation or injected during stop pending omitted; cancellation waits for stop ACK; one stop and zero pending |
| Adapter disabled while scanning | R scan `rapid_adapter_loss_stops_scan_before_recovery_without_auto_restart` | Covered in controlled shared-scan path: disabled/recovered before worker yield, explicit loss event, owned scanner stopped without auto restart; physical adapter pending |
| Adapter disabled while connected | D `adapter loss invalidates connections` | Covered: immediate disconnected snapshot, subsequent read rejected |
| Engine close while scan active | D `close joins an acknowledged active scan and closes its stream` | Covered in fake backend: advertisement delivery proves active scanning before close; stream done, zero tracked backend resources, cached close, new scan disposed |
| Engine close while connected | Same D case; G close during write | Covered: connection/resource retirement and disposed façade |
| Engine close while requests pending | Same D case; A `close with pending requests acknowledges native task termination` | Covered: failed future, idempotent close, pending count zero and future submits rejected |
| Engine close while event delivery pending | A `ordinary native event queued at close drains before port retirement; before=true/false` | Covered using actual engine and VM native post: queued before close or inside close entry, one copied ordinary event before stream done, pending request disposed, post after ACK rejected and resource counters at baseline |
| Repeated init/close cycles | A `100 repeated real FFI create/close cycles`; R engine stress | Covered: every cycle compares request/engine/worker counters to baseline |
| Connection recreated for same device | D fresh generations; G reconnect case | Covered: generation increases and new connection works |
| Old connection object after reconnect | D fresh generations; G disconnect/reconnect case | Covered: old object read rejected with disconnected |
| Unknown/stale handle through FFI | A `native connection registry rejects stale generations and malformed GATT commands`; unknown-handle case | Covered: controlled errors, invalid counters/handles, zero pending and idempotent close |

Every named race now has inspected deterministic library evidence in this table.
This is not completion of the full implementation objective. The crosswalk does not establish that all OS callbacks respect ownership,
that cancellation-time future destructors cannot panic, or that independent OS
resource counts return to baseline. Those require separate source/runtime audits.
