# Publishing checklist

## First public repository

1. Choose the final package/repository name and set `repository` and `issue_tracker`
   in `pubspec.yaml` to the actual GitHub destination.
2. Review `git ls-files --cached --others --exclude-standard` and run
   `python3 tool/repository_check.py`. Keep caches, signing keys, private hardware
   config, captures and local audit notes ignored.
3. Inspect author/committer emails in history. Use your chosen public email or
   GitHub noreply address before creating the public commit. Gitignore does not
   remove anything already committed.
4. Commit the required sources, Gradle wrapper, manifests/locks and licenses.
   Build/test a fresh checkout and run the full GitHub Actions matrix.
5. Enable GitHub private vulnerability reporting and review repository settings.
   Add a concise description and relevant Dart/Flutter/Bluetooth topics.
6. Publish as experimental with explicit runtime/hardware limitations. Link the
   passing Actions run; do not call unexecuted gates successful.

## Tags and package releases

Review version/changelog consistency, public API docs, CI and support claims.
Run a fresh dependency advisory check and all source/license verifiers.
For pub.dev, run `dart pub publish --dry-run` and inspect the complete archive.
Native sources, reviewed patches, lockfiles and license metadata must be included;
local evidence and private peripheral settings must be excluded.

Stable support additionally requires the native-runtime and controlled-hardware
gates in [implementation status](implementation-status.md).
