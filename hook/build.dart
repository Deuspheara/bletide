import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';
import 'package:native_toolchain_rust/native_toolchain_rust.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;
    output.dependencies.addAll([
      for (final file in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'])
        input.packageRoot.resolve('rust/$file'),
    ]);
    // Cargo's root dep-info does not list dependency-crate source files.
    // Include the pinned upstream patch so native asset cache invalidation also
    // sees edits to its Rust sources and manifests.
    final upstream = Directory.fromUri(
      input.packageRoot.resolve('rust/vendor/'),
    );
    output.dependencies.addAll([
      await for (final file in upstream.list(recursive: true))
        if (file is File) file.uri,
    ]);
    final testSupport = input.userDefines['test_support'];
    if (testSupport is! bool?) {
      throw const FormatException('bletide test_support must be a boolean');
    }
    final config = input.config.code;
    await RustBuilder(
      assetName: 'src/native/bindings.dart',
      features: testSupport == true ? const ['test-support'] : const [],
      extraCargoBuildArgs: const ['--locked'],
      extraCargoEnvironmentVariables: config.targetOS == OS.android
          ? androidApi24(config)
          : const {},
    ).run(input: input, output: output);
  });
}

// RustBuilder 1.0.6 defaults to API 35. Link against API 24 to match minSdk.
Map<String, String> androidApi24(CodeConfig config) {
  final triple = switch (config.targetArchitecture) {
    Architecture.arm => 'armv7-linux-androideabi',
    Architecture.arm64 => 'aarch64-linux-android',
    Architecture.x64 => 'x86_64-linux-android',
    _ => throw UnsupportedError('Unsupported Android architecture'),
  };
  final ndkTriple = triple.replaceFirst('armv7-', 'armv7a-');
  final directory = config.cCompiler!.compiler.resolve('.');
  final suffix = Platform.isWindows ? '.cmd' : '';
  String compiler(String tail) {
    final uri = directory.resolve('$ndkTriple$tail$suffix');
    if (!File.fromUri(uri).existsSync()) {
      throw StateError('Android API 24 compiler not found: $uri');
    }
    return uri.toFilePath();
  }

  final key = triple.replaceAll('-', '_');
  return {
    'CC_$key': compiler('24-clang'),
    'CXX_$key': compiler('24-clang++'),
    'CARGO_TARGET_${key.toUpperCase()}_LINKER': compiler('24-clang'),
  };
}
