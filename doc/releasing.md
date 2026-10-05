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

## Proposed GitHub metadata and owner actions

Suggested description:

> Experimental BLE central/client transport for Dart and Flutter, using Rust,
> btleplug and Tokio on native platforms and Web Bluetooth in browsers.

Suggested topics: `dart`, `flutter`, `bluetooth`, `bluetooth-low-energy`, `ble`,
`rust`, `btleplug`, `tokio`, `ffi`, `web-bluetooth`.

These are proposals; local source edits do not apply repository settings.
An owner should:

- Set the About description/topics and keep `main` as the default branch.
- Review a main-branch ruleset requiring pull-request review, resolved review
  threads and the three `native-contracts` matrix checks (macOS, Ubuntu, Windows).
  Select the actual reported check names after a complete successful Actions run;
  block force pushes/deletion and review owner bypass permissions.
- Enable private vulnerability reporting and available secret scanning/push
  protection. Inspect history and existing public artifacts independently of the
  working-tree hygiene check; it does not scan every secret format or history.
- Keep Actions token permissions read-only by default and restrict third-party
  actions to reviewed, pinned revisions. This workflow already requests only
  `contents: read` and pins action SHAs; account-level settings still need review.
- Keep Issues available for reproducible bugs/hardware evidence. Disable the Wiki
  if documentation is maintained only in `doc`; enable Discussions only if there
  is an owner willing to maintain them. Neither is a release requirement.
- Review merge methods/automatic branch deletion for contributor workflow and
  public author metadata. Do not rewrite existing history as routine cleanup.

The uncommitted audit cannot have a current remote Actions result without being
submitted to GitHub. Local host and artifact checks are documented separately
from that remaining matrix gate and from physical runtime validation.

Stable support additionally requires the native-runtime and controlled-hardware
gates in [implementation status](implementation-status.md).
