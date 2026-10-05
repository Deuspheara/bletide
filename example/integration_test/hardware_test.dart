import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:bletide/bletide.dart';

import '../../integration_test/hardware_config.dart';
import '../../integration_test/hardware_scenario.dart';

void main() {
  final binding = IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets(
    'configured peripheral lifecycle',
    (tester) async {
      final checks = <Map<String, Object?>>[];
      binding.reportData = {
        'platform': defaultTargetPlatform.name,
        'osVersion': Platform.operatingSystemVersion,
        'dartVersion': Platform.version,
        'startedAt': DateTime.now().toUtc().toIso8601String(),
        'checks': checks,
        'status': 'running',
      };
      await tester.pumpWidget(
        const MaterialApp(
          home: Scaffold(
            body: Center(child: Text('bletide hardware verification running')),
          ),
        ),
      );
      try {
        // Validate every UUID and safe payload before opening the native engine.
        final config = HardwareConfig.fromEnvironment();
        binding.reportData!['hardware'] = config.hardware;
        Object? failure;
        StackTrace? failureStack;
        await tester.runAsync(() async {
          try {
            await runHardwareScenario(
              Ble(),
              config,
              record: (check, evidence) =>
                  checks.add({'check': check, ...evidence}),
            );
          } catch (error, stack) {
            // WidgetTester.runAsync reports uncaught callback errors separately;
            // capture them so report status cannot accidentally become passed.
            failure = error;
            failureStack = stack;
          }
        });
        if (failure != null) Error.throwWithStackTrace(failure!, failureStack!);
        binding.reportData!['status'] = 'passed';
      } catch (_) {
        binding.reportData!['status'] = 'failed';
        rethrow;
      } finally {
        binding.reportData!['finishedAt'] = DateTime.now()
            .toUtc()
            .toIso8601String();
      }
    },
    skip: kIsWeb,
    timeout: const Timeout(Duration(minutes: 15)),
  );
}
