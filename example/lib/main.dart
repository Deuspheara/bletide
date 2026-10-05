import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:bletide/bletide.dart';

void main() {
  LicenseRegistry.addLicense(() async* {
    final notices = await rootBundle.loadString(
      'packages/bletide/THIRD_PARTY_NOTICES',
    );
    final licenses = await rootBundle.loadString(
      'packages/bletide/THIRD_PARTY_LICENSES',
    );
    yield LicenseEntryWithLineBreaks([
      'bletide native dependencies',
    ], '$notices\n\n$licenses');
  });
  runApp(const MaterialApp(home: BleTestbench()));
}

class BleTestbench extends StatefulWidget {
  const BleTestbench({super.key, this.ble});
  final Ble? ble;
  @override
  State<BleTestbench> createState() => _BleTestbenchState();
}

class _BleTestbenchState extends State<BleTestbench> {
  late final Ble _ble = widget.ble ?? Ble();
  final Map<BleDeviceId, BleAdvertisement> _devices = {};
  final List<String> _events = [];
  final TextEditingController _write = TextEditingController();
  final TextEditingController _servicePermissions = TextEditingController();
  StreamSubscription<BleAdvertisement>? _scan;
  StreamSubscription<BleDiagnostic>? _diagnostics;
  StreamSubscription<BleConnectionState>? _connectionStates;
  final Map<String, StreamSubscription<Uint8List>> _notifications = {};
  final Set<String> _notificationSetup = {};
  BleConnection? _connection;
  List<BleService> _services = [];
  BleAdapterState _adapter = BleAdapterState.unavailable;
  StreamSubscription<BleAdapterState>? _adapterStates;
  bool _ready = false;
  bool _closed = false;
  bool _disposing = false;
  BleCancellation? _connectCancellation;

  @override
  void initState() {
    super.initState();
    _adapterStates = _ble.adapterState.listen((state) {
      if (mounted) setState(() => _adapter = state);
    });
    _diagnostics = _ble.diagnostics.listen(
      (event) => _log(
        '${event.timestamp.toIso8601String()} ${event.name} ${event.deviceId ?? ''} ${event.errorCode ?? ''}${event.duration == null ? '' : ' ${event.duration!.inMilliseconds}ms'}',
      ),
    );
    unawaited(
      _action(() async {
        await _ble.ready;
        if (mounted) setState(() => _ready = true);
      }),
    );
  }

  String _hex(List<int> bytes) =>
      bytes.map((byte) => byte.toRadixString(16).padLeft(2, '0')).join(' ');
  void _log(String message) {
    if (!mounted || _disposing) return;
    setState(() {
      _events.insert(0, message);
      if (_events.length > 100) _events.removeLast();
    });
  }

  Future<void> _action(Future<void> Function() action) async {
    try {
      await action();
    } catch (error) {
      _log('$error');
    }
  }

  Future<void> _toggleScan() async {
    if (_scan != null) {
      final scan = _scan!;
      setState(() => _scan = null);
      await scan.cancel();
    } else {
      setState(() {
        _scan = _ble.scan().listen(
          (advertisement) {
            if (mounted) {
              setState(() => _devices[advertisement.deviceId] = advertisement);
            }
          },
          onError: (Object error) => _log('$error'),
          onDone: () {
            if (mounted) setState(() => _scan = null);
          },
        );
      });
    }
  }

  Future<void> _connect(BleDeviceId device) async {
    if (_connectCancellation != null) return;
    final cancellation = BleCancellation();
    setState(() => _connectCancellation = cancellation);
    try {
      await _disconnect();
      final connection = await _ble.connect(device, cancellation: cancellation);
      if (!mounted || _disposing || _closed || cancellation.isCancelled) {
        await connection.disconnect();
        return;
      }
      setState(() {
        _connection = connection;
        _services = [];
      });
      _connectionStates = connection.states.listen((state) {
        _log('Connection state: ${state.name}');
        if (state == BleConnectionState.disconnected &&
            identical(_connection, connection)) {
          unawaited(_action(_disconnect));
        }
      });
    } finally {
      _connectCancellation = null;
      if (mounted && !_disposing) setState(() {});
    }
  }

  Future<void> _disconnect() async {
    final connection = _connection;
    final listeners = <StreamSubscription<dynamic>>[
      ..._notifications.values,
      ?_connectionStates,
    ];
    _notifications.clear();
    _connectionStates = null;
    // Detach this generation before awaiting cleanup. A pending notification
    // setup must not attach a listener to it after the snapshot above.
    if (mounted) {
      setState(() {
        _connection = null;
        _services = [];
      });
    }
    for (final listener in listeners) {
      try {
        await listener.cancel();
      } catch (error) {
        _log('$error');
      }
    }
    await connection?.disconnect();
  }

  Future<void> _discover() async {
    final connection = _connection!;
    final services = await connection.discoverServices();
    if (mounted &&
        !_disposing &&
        identical(_connection, connection) &&
        connection.state == BleConnectionState.connected) {
      setState(() => _services = services);
    }
  }

  Uint8List _parseHex() {
    final compact = _write.text.replaceAll(RegExp(r'\s'), '');
    if (compact.length.isOdd || !RegExp(r'^[0-9a-fA-F]*$').hasMatch(compact)) {
      throw const FormatException('Enter an even number of hexadecimal digits');
    }
    return Uint8List.fromList([
      for (var i = 0; i < compact.length; i += 2)
        int.parse(compact.substring(i, i + 2), radix: 16),
    ]);
  }

  Future<void> _toggleNotifications(BleCharacteristic characteristic) async {
    if (!_notificationSetup.add(characteristic.key)) return;
    if (mounted) setState(() {});
    try {
      final existing = _notifications.remove(characteristic.key);
      if (existing != null) {
        await existing.cancel();
      } else {
        final connection = _connection!;
        final owner = await connection.enableNotifications(characteristic);
        if (!mounted ||
            !identical(_connection, connection) ||
            connection.state != BleConnectionState.connected) {
          await owner.cancel();
          return;
        }
        _notifications[characteristic.key] = owner.values.listen(
          (bytes) => _log('${characteristic.uuid}: ${_hex(bytes)}'),
          onError: (Object error) => _log('$error'),
        );
        _log('Notifications enabled: ${characteristic.uuid}');
      }
    } finally {
      _notificationSetup.remove(characteristic.key);
      if (mounted) setState(() {});
    }
  }

  Future<void> _close() async {
    try {
      await _ble.close();
    } finally {
      if (mounted) {
        setState(() {
          _closed = true;
          _ready = false;
          _scan = null;
          _connection = null;
          _services = [];
        });
      }
    }
  }

  Future<void> _disposeBle() async {
    try {
      // Close physical ownership before canceling listeners, so cancellation
      // cannot start new stop/unsubscribe operations against a closing backend.
      // A terminal close error was already reported by the explicit action.
      // Avoid replaying that cached error when disposing the closed testbench.
      if (!_closed) await _ble.close();
    } catch (error, stack) {
      FlutterError.reportError(
        FlutterErrorDetails(
          exception: error,
          stack: stack,
          library: 'Bletide testbench',
          context: ErrorDescription('while closing the BLE engine'),
        ),
      );
    } finally {
      await _adapterStates?.cancel();
      await _diagnostics?.cancel();
      await _connectionStates?.cancel();
      await _scan?.cancel();
      for (final subscription in _notifications.values.toList()) {
        await subscription.cancel();
      }
    }
  }

  @override
  void dispose() {
    _disposing = true;
    _connectCancellation?.cancel();
    unawaited(_disposeBle());
    _write.dispose();
    _servicePermissions.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final capabilities = _ble.capabilities;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Bletide testbench'),
        actions: [
          PopupMenuButton<String>(
            tooltip: 'Engine actions',
            onSelected: (action) {
              if (action == 'licenses') {
                showLicensePage(
                  context: context,
                  applicationName: 'Bletide testbench',
                );
              } else if (action == 'close') {
                _action(_close);
              }
            },
            itemBuilder: (_) => [
              const PopupMenuItem(value: 'licenses', child: Text('Licenses')),
              PopupMenuItem(
                value: 'close',
                enabled: !_closed,
                child: const Text('Close engine'),
              ),
            ],
          ),
        ],
      ),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          Text(
            'Adapter: ${_adapter.name} • ${_closed
                ? 'closed'
                : _ready
                ? 'ready'
                : 'initializing'}',
          ),
          Text(
            'Capabilities: scan=${capabilities.scan}, GATT=${capabilities.gatt}, descriptors=${capabilities.descriptorAccess}, RSSI=${capabilities.readRssi}, MTU=${capabilities.getMtu}, request MTU=${capabilities.requestMtu}, priority=${capabilities.requestConnectionPriority}',
          ),
          if (capabilities.requestDevice)
            TextField(
              controller: _servicePermissions,
              decoration: const InputDecoration(
                labelText: 'Service UUIDs to allow (comma-separated)',
              ),
            ),
          Wrap(
            spacing: 8,
            children: [
              if (capabilities.requestDevice)
                FilledButton(
                  onPressed: _closed
                      ? null
                      : () => _action(() async {
                          final selected = await _ble.requestDevice(
                            options: BleDeviceRequest(
                              optionalServices: _servicePermissions.text
                                  .split(',')
                                  .map((value) => value.trim())
                                  .where((value) => value.isNotEmpty)
                                  .map(BleUuid.new),
                            ),
                          );
                          if (mounted) {
                            setState(
                              () => _devices[selected.deviceId] = selected,
                            );
                          }
                        }),
                  child: const Text('Choose device'),
                ),
              FilledButton(
                onPressed: _ready && capabilities.scan
                    ? () => _action(_toggleScan)
                    : null,
                child: Text(_scan == null ? 'Start scan' : 'Stop scan'),
              ),
              if (_connectCancellation != null)
                OutlinedButton(
                  onPressed: _connectCancellation!.cancel,
                  child: const Text('Cancel connect'),
                ),
              OutlinedButton(
                onPressed: _connection == null || _connectCancellation != null
                    ? null
                    : () => _action(_disconnect),
                child: const Text('Disconnect'),
              ),
              OutlinedButton(
                onPressed: _connection == null
                    ? null
                    : () => _action(_discover),
                child: const Text('Discover services'),
              ),
              OutlinedButton(
                onPressed: _connection != null && capabilities.readRssi
                    ? () => _action(() async {
                        _log('RSSI: ${await _connection!.readRssi()}');
                      })
                    : null,
                child: const Text('Read RSSI'),
              ),
              OutlinedButton(
                onPressed: _connection != null && capabilities.getMtu
                    ? () => _action(() async {
                        final connection = _connection!;
                        _log(
                          'MTU: ${await connection.getMtu()}, conservative write payload: ${await connection.getWritePayloadLimit()} bytes',
                        );
                      })
                    : null,
                child: const Text('Get MTU'),
              ),
              if (capabilities.requestMtu)
                PopupMenuButton<int>(
                  tooltip: 'Request MTU',
                  enabled: _connection != null,
                  onSelected: (mtu) => _action(() async {
                    final connection = _connection!;
                    final effective = await connection.requestMtu(mtu);
                    _log('Requested MTU $mtu; effective MTU: $effective');
                  }),
                  itemBuilder: (_) => [
                    for (final mtu in [23, 185, 247, 517])
                      PopupMenuItem(value: mtu, child: Text('MTU $mtu')),
                  ],
                  child: Padding(
                    padding: const EdgeInsets.all(12),
                    child: Text(
                      'Request MTU',
                      style: TextStyle(
                        color: _connection == null
                            ? Theme.of(context).disabledColor
                            : Theme.of(context).colorScheme.primary,
                      ),
                    ),
                  ),
                ),
              if (capabilities.requestConnectionPriority)
                PopupMenuButton<BleConnectionPriority>(
                  tooltip: 'Connection priority',
                  enabled: _connection != null,
                  onSelected: (priority) => _action(() async {
                    await _connection!.requestConnectionPriority(priority);
                    _log('Priority ${priority.name} requested (OS hint)');
                  }),
                  itemBuilder: (_) => [
                    for (final priority in BleConnectionPriority.values)
                      PopupMenuItem(
                        value: priority,
                        child: Text(priority.name),
                      ),
                  ],
                  child: Padding(
                    padding: const EdgeInsets.all(12),
                    child: Text(
                      'Connection priority',
                      style: TextStyle(
                        color: _connection == null
                            ? Theme.of(context).disabledColor
                            : Theme.of(context).colorScheme.primary,
                      ),
                    ),
                  ),
                ),
            ],
          ),
          if (_connection != null)
            Text(
              'Device: ${_connection!.deviceId} • generation ${_connection!.generation} • ${_connection!.state.name}',
            ),
          for (final advertisement in _devices.values)
            ListTile(
              title: Text(advertisement.name ?? 'Unnamed device'),
              subtitle: Text(
                '${advertisement.deviceId} • RSSI ${advertisement.rssi ?? 'unknown'}\nServices: ${advertisement.serviceUuids.join(', ')}\nManufacturer: ${advertisement.manufacturerData.map((id, bytes) => MapEntry(id, _hex(bytes)))}\nService data: ${advertisement.serviceData.map((id, bytes) => MapEntry(id, _hex(bytes)))}',
              ),
              trailing: OutlinedButton(
                onPressed:
                    _ready &&
                        capabilities.connect &&
                        _connectCancellation == null
                    ? () => _action(() => _connect(advertisement.deviceId))
                    : null,
                child: const Text('Connect'),
              ),
            ),
          const Divider(),
          TextField(
            controller: _write,
            decoration: const InputDecoration(
              labelText: 'Write value (hex bytes or UTF-8 text)',
            ),
          ),
          for (final service in _services)
            ExpansionTile(
              title: Text('Service ${service.uuid}'),
              children: [
                for (final characteristic in service.characteristics)
                  ExpansionTile(
                    title: Text(
                      '${characteristic.uuid} • properties 0x${characteristic.properties.bits.toRadixString(16)}',
                    ),
                    children: [
                      Wrap(
                        spacing: 8,
                        children: [
                          TextButton(
                            onPressed: characteristic.properties.read
                                ? () => _action(() async {
                                    _log(
                                      'Read ${characteristic.uuid}: ${_hex(await _connection!.read(characteristic))}',
                                    );
                                  })
                                : null,
                            child: const Text('Read'),
                          ),
                          for (final response in [true, false])
                            TextButton(
                              onPressed:
                                  (response
                                      ? characteristic.properties.write
                                      : characteristic
                                            .properties
                                            .writeWithoutResponse)
                                  ? () => _action(
                                      () => _connection!.write(
                                        characteristic,
                                        _parseHex(),
                                        withResponse: response,
                                      ),
                                    )
                                  : null,
                              child: Text(
                                response
                                    ? 'Write hex'
                                    : 'Write hex no response',
                              ),
                            ),
                          TextButton(
                            onPressed: characteristic.properties.write
                                ? () => _action(
                                    () => _connection!.write(
                                      characteristic,
                                      Uint8List.fromList(
                                        utf8.encode(_write.text),
                                      ),
                                    ),
                                  )
                                : null,
                            child: const Text('Write UTF-8'),
                          ),
                          TextButton(
                            onPressed:
                                !_notificationSetup.contains(
                                      characteristic.key,
                                    ) &&
                                    (characteristic.properties.notify ||
                                        characteristic.properties.indicate)
                                ? () => _action(
                                    () => _toggleNotifications(characteristic),
                                  )
                                : null,
                            child: Text(
                              _notificationSetup.contains(characteristic.key)
                                  ? 'Updating notifications…'
                                  : _notifications.containsKey(
                                      characteristic.key,
                                    )
                                  ? 'Unsubscribe'
                                  : 'Subscribe',
                            ),
                          ),
                        ],
                      ),
                      for (final descriptor in characteristic.descriptors)
                        ListTile(
                          title: Text('Descriptor ${descriptor.uuid}'),
                          trailing: Wrap(
                            children: [
                              TextButton(
                                onPressed: capabilities.descriptorAccess
                                    ? () => _action(() async {
                                        _log(
                                          'Descriptor ${descriptor.uuid}: ${_hex(await _connection!.readDescriptor(descriptor))}',
                                        );
                                      })
                                    : null,
                                child: const Text('Read'),
                              ),
                              TextButton(
                                onPressed: capabilities.descriptorAccess
                                    ? () => _action(
                                        () => _connection!.writeDescriptor(
                                          descriptor,
                                          _parseHex(),
                                        ),
                                      )
                                    : null,
                                child: const Text('Write hex'),
                              ),
                            ],
                          ),
                        ),
                    ],
                  ),
              ],
            ),
          const Divider(),
          const Text('Recent diagnostics and results'),
          for (final event in _events) SelectableText(event),
        ],
      ),
    );
  }
}
