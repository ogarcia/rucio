<!--
Thanks for contributing to rucio! Please fill in the sections below and tick
the checklist. Keep the PR focused — one logical change is easier to review.
-->

## What does this PR do?

<!-- A short description of the change and why it's needed. -->

## Related issues

<!-- e.g. "Closes #123" / "Refs #45". Delete if none. -->

## How was it tested?

<!-- Commands run, manual steps, platforms checked (Linux/macOS, container, etc.). -->

## Checklist

These mirror the `.githooks/pre-commit` hook — run them locally before pushing
to avoid CI round-trips. (Activate the hook once with
`git config core.hooksPath .githooks`.)

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes (native workspace)
- [ ] `cargo clippy -p rucio-web --target wasm32-unknown-unknown -- -D warnings` passes (wasm frontend)
- [ ] `cargo test` passes for the crates I touched
- [ ] If frontend changed: the panel builds (`cd rucio-web && trunk build --release`)
- [ ] Commits follow Conventional Commits and the change is documented where relevant (docs/, README, comments)
