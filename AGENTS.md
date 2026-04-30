# Repository Guidelines

## Project Layout

Relus is a Rust workspace for data synchronization tooling.

- `cli/`: command-line entry point (`relus-cli`) and example user configs in `cli/user_config/`.
- `core/`: sync engine, scheduler, API, runner, DSL engine, examples, benches, and design docs.
- `common/`: shared config, logging, data types, pipeline messages, and interfaces.
- `reader/`: source reader traits and RDBMS/binlog/API reader implementations.
- `writer/`: destination writer traits and RDBMS writer utilities.
- `connector-rdbms/`: SQL/database connector, schema discovery, metadata, and pool helpers.

## Common Commands

From the repository root:

```bash
cargo build
cargo build --release --bin relus-cli
cargo run -p relus_cli -- sync -c cli/user_config/default_job.json
cargo test
cargo fmt
cargo clippy --workspace --all-targets
```

The README currently shows `--bin cli`, but the CLI package defines the binary as `relus-cli`.

## Development Notes

- Prefer workspace-level commands from the repository root unless a crate-specific workflow is needed.
- Keep shared data contracts in `common/`; avoid duplicating type definitions in reader/writer/core crates.
- Keep database-specific SQL and schema logic in `connector-rdbms/` or crate-local `rdbms_*_util` modules as appropriate.
- Treat `cli/user_config/*.json` as runnable examples; avoid committing real credentials.
- There may be existing user edits in the worktree. Do not revert unrelated changes while making task-specific updates.

## Verification

For narrow Rust changes, run the smallest relevant check first, for example:

```bash
cargo check -p relus_core
```

Before broader handoff, prefer:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
```
