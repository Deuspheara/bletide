import 'dart:io';
import 'dart:typed_data';
import 'dart:ui' as ui;

import 'package:flutter/material.dart';
import 'package:flutter/rendering.dart';
import 'package:flutter/services.dart' show rootBundle;
import 'package:flutter_test/flutter_test.dart';
import 'package:bletide/bletide.dart';
import 'package:bletide/testing.dart';
import 'package:bletide_example/main.dart';

const first = BleDeviceId('test-first');
const second = BleDeviceId('test-second');

Future<(FakeBleBackend, Ble)> start(
  WidgetTester tester, {
  FakeBleBackend? backend,
}) async {
  backend ??= FakeBleBackend();
  final ble = Ble(backend: backend);
  await tester.pumpWidget(
    RepaintBoundary(
      key: const ValueKey('testbench-capture'),
      child: MaterialApp(
        debugShowCheckedModeBanner: false,
        theme:
            Platform.environment.containsKey('OPENBLE_TESTBENCH_CAPTURE_FONT')
            ? ThemeData(fontFamily: 'Roboto')
            : null,
        home: BleTestbench(ble: ble),
      ),
    ),
  );
  await settle(tester);
  await tester.tap(find.text('Start scan'));
  await tester.pump();
  backend.next<void>('scan.start').complete(null);
  await tester.pump();
  backend.advertise(BleAdvertisement(deviceId: first, name: 'First'));
  backend.advertise(BleAdvertisement(deviceId: second, name: 'Second'));
  await tester.pump();
  return (backend, ble);
}

Future<(FakeBackendConnection, BleCharacteristic)> discover(
  WidgetTester tester,
  FakeBleBackend backend, {
  BleCharacteristic? attribute,
}) async {
  await tester.tap(find.text('Connect').first);
  await tester.pump();
  final connection = backend.completeConnect(first);
  await settle(tester);
  await tester.tap(find.text('Discover services'));
  await tester.pump();
  final characteristic =
      attribute ??
      BleCharacteristic(
        serviceUuid: BleUuid('180f'),
        uuid: BleUuid('2a19'),
        properties: const BleCharacteristicProperties(0x12),
      );
  backend.next<List<BleService>>('discover').complete([
    BleService(
      uuid: characteristic.serviceUuid,
      primary: true,
      characteristics: [characteristic],
    ),
  ]);
  await settle(tester);
  final service = find.text('Service ${characteristic.serviceUuid}');
  await tester.scrollUntilVisible(
    service,
    200,
    scrollable: find
        .descendant(
          of: find.byType(ListView).first,
          matching: find.byType(Scrollable),
        )
        .first,
  );
  await tester.tap(service);
  await settle(tester);
  final characteristicTile = find.textContaining(
    '${characteristic.uuid} • properties',
  );
  await tester.ensureVisible(characteristicTile);
  await tester.tap(characteristicTile);
  await settle(tester);
  await tester.ensureVisible(find.text('Subscribe'));
  return (connection, characteristic);
}

Future<void> settle(WidgetTester tester) async {
  // Stream cancellation may enqueue real event-loop work. Yield one event turn
  // without a sleep or advancing virtual time, then render the resulting state.
  await tester.runAsync(() => Future<void>(() {}));
  await tester.pumpAndSettle();
}

Future<void> finish(WidgetTester tester, Ble ble) async {
  await tester.runAsync(ble.close);
  await tester.pumpWidget(const SizedBox());
  await settle(tester);
}

Future<void> capture(WidgetTester tester, String name) async {
  final directory = Platform.environment['OPENBLE_TESTBENCH_CAPTURE_DIR'];
  if (directory == null) return;
  final boundary = tester.renderObject<RenderRepaintBoundary>(
    find.byKey(const ValueKey('testbench-capture')),
  );
  await tester.runAsync(() async {
    final image = await boundary.toImage(pixelRatio: 1);
    try {
      final data = await image.toByteData(format: ui.ImageByteFormat.png);
      if (data == null) throw StateError('Could not encode testbench frame');
      await Directory(directory).create(recursive: true);
      await File('$directory/$name.png')
          .writeAsBytes(data.buffer.asUint8List());
    } finally {
      image.dispose();
    }
  });
}

void main() {
  setUpAll(() async {
    final font = Platform.environment['OPENBLE_TESTBENCH_CAPTURE_FONT'];
    if (font != null) {
      await ui.loadFontFromList(
        await File(font).readAsBytes(),
        fontFamily: 'Roboto',
      );
      final icons = await rootBundle.load('fonts/MaterialIcons-Regular.otf');
      await ui.loadFontFromList(
        icons.buffer.asUint8List(icons.offsetInBytes, icons.lengthInBytes),
        fontFamily: 'MaterialIcons',
      );
    }
  });
  testWidgets('link controls await results and retain failure causes', (
    tester,
  ) async {
    await tester.binding.setSurfaceSize(const Size(1000, 1600));
    addTearDown(() => tester.binding.setSurfaceSize(null));
    final (backend, ble) = await start(tester);
    expect(
      tester
          .widget<PopupMenuButton<int>>(find.byType(PopupMenuButton<int>))
          .enabled,
      isFalse,
    );
    expect(
      tester
          .widget<PopupMenuButton<BleConnectionPriority>>(
            find.byType(PopupMenuButton<BleConnectionPriority>),
          )
          .enabled,
      isFalse,
    );
    await tester.tap(find.text('Connect').first);
    await tester.pump();
    backend.completeConnect(first);
    await settle(tester);

    await tester.tap(find.text('Read RSSI'));
    await tester.pump();
    backend.next<int>('rssi').complete(-73);
    await settle(tester);
    expect(find.text('RSSI: -73'), findsOneWidget);

    await tester.tap(find.text('Get MTU'));
    await tester.pump();
    backend.next<int>('getMtu').complete(517);
    await tester.pump();
    backend.next<int>('getMtu').complete(517);
    await settle(tester);
    expect(
      find.text('MTU: 517, conservative write payload: 512 bytes'),
      findsOneWidget,
    );

    await tester.tap(find.byTooltip('Request MTU'));
    await settle(tester);
    await tester.tap(find.text('MTU 247'));
    await settle(tester);
    expect(find.text('Requested MTU 247; effective MTU: 185'), findsNothing);
    backend.next<int>('requestMtu').complete(185);
    await settle(tester);
    expect(find.text('Requested MTU 247; effective MTU: 185'), findsOneWidget);

    for (final priority in BleConnectionPriority.values) {
      await tester.tap(find.byTooltip('Connection priority'));
      await settle(tester);
      await tester.tap(find.text(priority.name));
      await settle(tester);
      final request = backend.next<void>('connection.priority');
      expect(request.value, [priority.index]);
      expect(
        find.text('Priority ${priority.name} requested (OS hint)'),
        findsNothing,
      );
      request.complete(null);
      await settle(tester);
      expect(
        find.text('Priority ${priority.name} requested (OS hint)'),
        findsOneWidget,
      );
    }

    await tester.tap(find.byTooltip('Request MTU'));
    await settle(tester);
    await tester.tap(find.text('MTU 517'));
    await settle(tester);
    backend
        .next<int>('requestMtu')
        .fail(
          const BleException(
            BleErrorCode.gattFailure,
            'MTU rejected by fixture',
          ),
        );
    await settle(tester);
    expect(find.textContaining('MTU rejected by fixture'), findsOneWidget);
    expect(find.textContaining('Requested MTU 517; effective'), findsNothing);
    await tester.tap(find.byTooltip('Request MTU'));
    await settle(tester);
    await tester.tap(find.text('MTU 23'));
    await settle(tester);
    backend.next<int>('requestMtu').complete(23);
    await settle(tester);
    expect(find.text('Requested MTU 23; effective MTU: 23'), findsOneWidget);
    expect(tester.takeException(), isNull);
    await finish(tester, ble);
    expect(backend.pending, isEmpty);
    expect(backend.liveConnections, 0);
  });

  testWidgets('unsupported link controls never issue backend requests', (
    tester,
  ) async {
    final (backend, ble) = await start(
      tester,
      backend: FakeBleBackend(
        capabilities: const BleCapabilities(
          scan: true,
          connect: true,
          gatt: true,
        ),
      ),
    );
    await tester.tap(find.text('Connect').first);
    await tester.pump();
    backend.completeConnect(first);
    await settle(tester);
    expect(find.byTooltip('Request MTU'), findsNothing);
    expect(find.byTooltip('Connection priority'), findsNothing);
    for (final label in ['Read RSSI', 'Get MTU']) {
      expect(
        tester
            .widget<OutlinedButton>(find.widgetWithText(OutlinedButton, label))
            .onPressed,
        isNull,
      );
      await tester.tap(find.text(label));
    }
    await tester.pump();
    expect(backend.pending, isEmpty);
    await finish(tester, ble);
  });

  testWidgets('failed engine close retires controls and preserves its cause', (
    tester,
  ) async {
    await tester.binding.setSurfaceSize(const Size(1000, 1600));
    addTearDown(() => tester.binding.setSurfaceSize(null));
    final (backend, _) = await start(
      tester,
      backend: FakeBleBackend(
        closeError: const BleException(
          BleErrorCode.internal,
          'Controlled engine cleanup failure',
        ),
      ),
    );
    await discover(tester, backend);
    await tester.runAsync(() => tester.tap(find.text('Subscribe')));
    await tester.pump();
    backend.next<void>('subscribe').complete(null);
    await settle(tester);
    expect(backend.subscriptions, 1);
    await tester.tap(find.byTooltip('Engine actions'));
    await settle(tester);
    await tester.tap(find.text('Close engine'));
    await settle(tester);
    expect(find.text('Adapter: ready • closed'), findsOneWidget);
    expect(
      find.textContaining('Controlled engine cleanup failure'),
      findsOneWidget,
    );
    expect(find.textContaining('Device: $first • generation'), findsNothing);
    expect(find.text('Service ${BleUuid('180f')}'), findsNothing);
    expect(
      tester
          .widget<FilledButton>(find.widgetWithText(FilledButton, 'Start scan'))
          .onPressed,
      isNull,
    );
    for (final button in tester.widgetList<OutlinedButton>(
      find.widgetWithText(OutlinedButton, 'Connect'),
    )) {
      expect(button.onPressed, isNull);
    }
    expect(backend.pending, isEmpty);
    expect(backend.liveConnections, 0);
    expect(backend.subscriptions, 0);
    expect(backend.scanning, isFalse);
    await tester.pumpWidget(const SizedBox());
    await settle(tester);
    expect(tester.takeException(), isNull);
  });

  testWidgets(
    'scanner metadata remains usable on narrow and large-text screens',
    (tester) async {
      addTearDown(() => tester.binding.setSurfaceSize(null));
      addTearDown(tester.platformDispatcher.clearTextScaleFactorTestValue);
      for (final scale in [1.0, 2.0]) {
        await tester.binding.setSurfaceSize(const Size(390, 844));
        tester.platformDispatcher.textScaleFactorTestValue = scale;
        final (backend, ble) = await start(tester);
        final service = BleUuid('180f');
        backend.advertise(
          BleAdvertisement(
            deviceId: first,
            name: 'Fixture sensor',
            rssi: -61,
            serviceUuids: [service],
            manufacturerData: {
              0x004c: Uint8List.fromList([0, 128, 255]),
            },
            serviceData: {
              service: Uint8List.fromList([65, 0, 255]),
            },
          ),
        );
        await settle(tester);
        await tester.scrollUntilVisible(
          find.text('Fixture sensor'),
          200,
          scrollable: find
              .descendant(
                of: find.byType(ListView).first,
                matching: find.byType(Scrollable),
              )
              .first,
        );
        expect(find.text('First'), findsNothing);
        expect(find.text('Fixture sensor'), findsOneWidget);
        for (final field in [
          '$first • RSSI -61',
          'Services: $service',
          'Manufacturer: {76: 00 80 ff}',
          'Service data: {$service: 41 00 ff}',
        ]) {
          expect(find.textContaining(field), findsOneWidget);
        }
        final connect = find.descendant(
          of: find.widgetWithText(ListTile, 'Fixture sensor'),
          matching: find.widgetWithText(OutlinedButton, 'Connect'),
        );
        expect(tester.widget<OutlinedButton>(connect).onPressed, isNotNull);
        expect(tester.takeException(), isNull);
        await capture(tester, 'scanner-scale-${scale.toInt()}');
        await tester.tap(find.byTooltip('Engine actions'));
        await settle(tester);
        expect(find.text('Licenses'), findsOneWidget);
        expect(find.text('Close engine'), findsOneWidget);
        expect(tester.takeException(), isNull);
        await capture(tester, 'engine-menu-scale-${scale.toInt()}');
        await tester.tap(find.text('Close engine'));
        await settle(tester);
        await tester.scrollUntilVisible(
          find.textContaining('Adapter:'),
          -200,
          scrollable: find
              .descendant(
                of: find.byType(ListView).first,
                matching: find.byType(Scrollable),
              )
              .first,
        );
        expect(find.textContaining('Adapter: ready • closed'), findsOneWidget);
        await tester.tap(find.byTooltip('Engine actions'));
        await settle(tester);
        expect(
          tester
              .widget<PopupMenuItem<String>>(
                find.widgetWithText(PopupMenuItem<String>, 'Close engine'),
              )
              .enabled,
          isFalse,
        );
        await finish(tester, ble);
        expect(backend.pending, isEmpty);
        expect(backend.liveConnections, 0);
        expect(backend.scanning, isFalse);
      }
    },
  );

  testWidgets(
    'GATT explorer sends binary and UTF-8 writes and descriptor requests',
    (tester) async {
      await tester.binding.setSurfaceSize(const Size(1000, 1600));
      addTearDown(() => tester.binding.setSurfaceSize(null));
      final (backend, ble) = await start(tester);
      final characteristic = BleCharacteristic(
        serviceUuid: BleUuid('180f'),
        uuid: BleUuid('2a19'),
        properties: const BleCharacteristicProperties(0x1e),
        descriptors: [
          BleDescriptor(
            serviceUuid: BleUuid('180f'),
            characteristicUuid: BleUuid('2a19'),
            uuid: BleUuid('2901'),
          ),
        ],
      );
      await discover(tester, backend, attribute: characteristic);
      await tester.tap(find.widgetWithText(TextButton, 'Read').first);
      await tester.pump();
      backend
          .next<Uint8List>('read')
          .complete(Uint8List.fromList([0, 128, 255]));
      await settle(tester);
      expect(
        find.text('Read ${characteristic.uuid}: 00 80 ff'),
        findsOneWidget,
      );

      await tester.enterText(find.byType(TextField), '00 80\nFF');
      await tester.tap(find.widgetWithText(TextButton, 'Write hex').first);
      await tester.pump();
      final write = backend.next<void>('write');
      expect(write.value, [0, 128, 255]);
      write.complete(null);
      await settle(tester);
      await tester.tap(find.text('Write hex no response'));
      await tester.pump();
      final noResponse = backend.next<void>('writeWithoutResponse');
      expect(noResponse.value, [0, 128, 255]);
      noResponse.complete(null);
      await settle(tester);

      await tester.enterText(find.byType(TextField), 'é🌍');
      await tester.tap(find.text('Write UTF-8'));
      await tester.pump();
      final text = backend.next<void>('write');
      expect(text.value, [0xc3, 0xa9, 0xf0, 0x9f, 0x8c, 0x8d]);
      text.complete(null);
      await settle(tester);

      await tester.tap(find.widgetWithText(TextButton, 'Read').last);
      await tester.pump();
      backend
          .next<Uint8List>('readDescriptor')
          .complete(Uint8List.fromList([0x41, 0, 0xff]));
      await settle(tester);
      expect(
        find.text(
          'Descriptor ${characteristic.descriptors.single.uuid}: 41 00 ff',
        ),
        findsOneWidget,
      );
      await tester.enterText(find.byType(TextField), 'a5 00');
      await tester.tap(find.widgetWithText(TextButton, 'Write hex').last);
      await tester.pump();
      final descriptorWrite = backend.next<void>('writeDescriptor');
      expect(descriptorWrite.value, [0xa5, 0]);
      descriptorWrite.complete(null);
      await settle(tester);
      expect(backend.pending, isEmpty);
      await capture(tester, 'gatt-explorer');
      await finish(tester, ble);
      expect(backend.liveConnections, 0);
      expect(backend.scanning, isFalse);
    },
  );
  testWidgets(
    'failed unsubscribe during disconnect does not prevent reconnect',
    (tester) async {
      final (backend, ble) = await start(tester);
      await discover(tester, backend);
      await tester.runAsync(() => tester.tap(find.text('Subscribe')));
      await tester.pump();
      backend.next<void>('subscribe').complete(null);
      await settle(tester);
      final scroll = tester
          .state<ScrollableState>(
            find
                .descendant(
                  of: find.byType(ListView),
                  matching: find.byType(Scrollable),
                )
                .first,
          )
          .position;
      scroll.jumpTo(scroll.minScrollExtent);
      await settle(tester);
      await tester.runAsync(() => tester.tap(find.text('Disconnect')));
      await settle(tester);
      backend
          .next<void>('unsubscribe')
          .fail(
            const BleException(
              BleErrorCode.gattFailure,
              'Controlled disable failure',
            ),
          );
      await settle(tester);
      backend.next<void>('disconnect').complete(null);
      await settle(tester);
      expect(
        tester
            .widget<OutlinedButton>(
              find.widgetWithText(OutlinedButton, 'Disconnect'),
            )
            .onPressed,
        isNull,
      );
      expect(find.textContaining('Device: $first • generation'), findsNothing);
      expect(backend.liveConnections, 0);
      expect(backend.subscriptions, 0);
      await tester.ensureVisible(find.text('Connect').last);
      await tester.tap(find.text('Connect').last);
      await tester.pump();
      backend.completeConnect(second);
      await settle(tester);
      expect(backend.liveConnections, 1);
      scroll.jumpTo(scroll.minScrollExtent);
      await settle(tester);
      expect(
        find.textContaining('Device: $second • generation'),
        findsOneWidget,
      );
      await tester.scrollUntilVisible(
        find.textContaining('Controlled disable failure'),
        200,
        scrollable: find
            .descendant(
              of: find.byType(ListView),
              matching: find.byType(Scrollable),
            )
            .first,
      );
      expect(find.textContaining('Controlled disable failure'), findsWidgets);
      await finish(tester, ble);
    },
  );
  testWidgets('rapid connects cannot leave an unrepresented connection', (
    tester,
  ) async {
    final (backend, ble) = await start(tester);
    // Both taps use the same rendered frame, before disabled controls rebuild.
    await tester.tap(find.text('Connect').first);
    await tester.tap(find.text('Connect').last);
    await tester.pump();
    expect(backend.history.where((name) => name == 'connect'), hasLength(1));
    backend.completeConnect(first);
    await settle(tester);
    expect(backend.liveConnections, 1);
    expect(find.textContaining('Device: $first • generation'), findsOneWidget);
    await finish(tester, ble);
  });

  for (final pendingDiscovery in [false, true]) {
    testWidgets(
      'remote loss retires the displayed generation (pending discovery: $pendingDiscovery)',
      (tester) async {
        await tester.binding.setSurfaceSize(const Size(1000, 1600));
        addTearDown(() => tester.binding.setSurfaceSize(null));
        final (backend, ble) = await start(tester);
        late FakeBackendConnection old;
        if (pendingDiscovery) {
          await tester.tap(find.text('Connect').first);
          await tester.pump();
          old = backend.completeConnect(first);
          await settle(tester);
          await tester.tap(find.text('Discover services'));
          await tester.pump();
          expect(
            backend.pending.where((op) => op.name == 'discover'),
            hasLength(1),
          );
        } else {
          final (connection, _) = await discover(tester, backend);
          old = connection;
          expect(find.text('Service ${BleUuid('180f')}'), findsOneWidget);
          await tester.runAsync(() => tester.tap(find.text('Subscribe')));
          await tester.pump();
          backend.next<void>('subscribe').complete(null);
          await settle(tester);
          expect(backend.subscriptions, 1);
        }
        old.remoteDisconnect();
        await settle(tester);
        expect(
          find.textContaining('Device: $first • generation'),
          findsNothing,
        );
        expect(find.text('Service ${BleUuid('180f')}'), findsNothing);
        for (final label in [
          'Disconnect',
          'Discover services',
          'Read RSSI',
          'Get MTU',
        ]) {
          expect(
            tester
                .widget<OutlinedButton>(
                  find.widgetWithText(OutlinedButton, label),
                )
                .onPressed,
            isNull,
          );
        }
        expect(backend.pending, isEmpty);
        expect(backend.liveConnections, 0);
        expect(backend.subscriptions, 0);
        await tester.tap(find.text('Connect').first);
        await tester.pump();
        final next = backend.completeConnect(first);
        await settle(tester);
        expect(next.generation, greaterThan(old.generation));
        expect(
          find.textContaining('Device: $first • generation ${next.generation}'),
          findsOneWidget,
        );
        expect(backend.liveConnections, 1);
        expect(backend.history.where((op) => op == 'scan.start'), hasLength(1));
        expect(tester.takeException(), isNull);
        await finish(tester, ble);
        expect(backend.pending, isEmpty);
        expect(backend.liveConnections, 0);
      },
    );
  }

  testWidgets('cancel connect cleans the attempt and permits immediate retry', (
    tester,
  ) async {
    final (backend, ble) = await start(tester);
    await tester.tap(find.text('Connect').first);
    await tester.pump();
    expect(backend.pending.where((op) => op.name == 'connect'), hasLength(1));
    await tester.tap(find.text('Cancel connect'));
    await settle(tester);
    expect(backend.pending.where((op) => op.name == 'connect'), isEmpty);
    expect(backend.liveConnections, 0);
    await tester.tap(find.text('Connect').last);
    await tester.pump();
    backend.completeConnect(second);
    await settle(tester);
    expect(backend.liveConnections, 1);
    expect(find.textContaining('Device: $second • generation'), findsOneWidget);
    await finish(tester, ble);
  });

  testWidgets('notification UI waits for ACK and awaits disable on cancel', (
    tester,
  ) async {
    final (backend, ble) = await start(tester);
    final (connection, characteristic) = await discover(tester, backend);
    await tester.runAsync(() => tester.tap(find.text('Subscribe')));
    await tester.pump();
    expect(find.text('Updating notifications…'), findsOneWidget);
    expect(find.textContaining('Notifications enabled:'), findsNothing);
    backend.next<void>('subscribe').complete(null);
    await settle(tester);
    expect(find.text('Unsubscribe'), findsOneWidget);
    expect(find.textContaining('Notifications enabled:'), findsOneWidget);
    connection.emitNotification(characteristic, Uint8List.fromList([0, 255]));
    await settle(tester);
    expect(find.text('${characteristic.uuid}: 00 ff'), findsOneWidget);
    await tester.ensureVisible(find.text('Unsubscribe'));
    await tester.runAsync(() => tester.tap(find.text('Unsubscribe')));
    await tester.pump();
    expect(find.text('Updating notifications…'), findsOneWidget);
    backend.next<void>('unsubscribe').complete(null);
    await settle(tester);
    expect(find.text('Subscribe'), findsOneWidget);
    expect(backend.subscriptions, 0);
    await finish(tester, ble);
  });

  testWidgets('failed notification setup reports cause and permits retry', (
    tester,
  ) async {
    final (backend, ble) = await start(tester);
    await discover(tester, backend);
    await tester.runAsync(() => tester.tap(find.text('Subscribe')));
    await tester.pump();
    backend
        .next<void>('subscribe')
        .fail(
          const BleException(
            BleErrorCode.gattFailure,
            'Controlled enable failure',
          ),
        );
    await settle(tester);
    expect(find.textContaining('Controlled enable failure'), findsOneWidget);
    expect(find.textContaining('Notifications enabled:'), findsNothing);
    expect(backend.subscriptions, 0);
    await tester.ensureVisible(find.text('Subscribe'));
    await settle(tester);
    final cleanup = backend.next<void>('unsubscribe');
    await tester.runAsync(() => tester.tap(find.text('Subscribe')));
    await tester.pump();
    expect(backend.pending.where((op) => op.name == 'subscribe'), isEmpty);
    cleanup.complete(null);
    await settle(tester);
    backend.next<void>('subscribe').complete(null);
    await settle(tester);
    expect(find.text('Unsubscribe'), findsOneWidget);
    expect(backend.subscriptions, 1);
    await finish(tester, ble);
  });
  testWidgets('widget disposal closes active scan and notification ownership', (
    tester,
  ) async {
    final (backend, ble) = await start(tester);
    await discover(tester, backend);
    await tester.runAsync(() => tester.tap(find.text('Subscribe')));
    await tester.pump();
    backend.next<void>('subscribe').complete(null);
    await settle(tester);
    expect(backend.subscriptions, 1);
    expect(backend.scanning, isTrue);
    await tester.runAsync(() => tester.pumpWidget(const SizedBox()));
    await tester.runAsync(ble.close);
    await settle(tester);
    expect(backend.pending, isEmpty);
    expect(backend.subscriptions, 0);
    expect(backend.liveConnections, 0);
    expect(backend.scanning, isFalse);
    expect(tester.takeException(), isNull);
  });
}
