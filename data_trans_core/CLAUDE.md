# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`data_trans_core` is a Rust library for synchronizing data from REST APIs to MySQL/PostgreSQL databases. It's part of a workspace that includes a CLI tool (`data_trans_cli`) and a Flutter desktop app (`my_app`).

**Core functionality:**
- Fetch JSON data from HTTP APIs
- Transform data using a DSL (Domain Specific Language) with functions like `upper()`, `concat()`, `coalesce()`
- Map API fields to database columns with type conversion
- Sync to MySQL/PostgreSQL with insert/upsert modes
- Batch processing with configurable transaction support
- HTTP API server for remote control

## Build and Test Commands

```bash
# Build the library
cargo build

# Build with release optimizations
cargo build --release

# Run tests
cargo test

# Run a specific example
cargo run --example test_sync_command_new

# Build entire workspace (from parent directory)
cd .. && cargo build --workspace

# Run the HTTP server (from examples or via lib)
cargo run --example test_sync_command_new
```

## Configuration System

The project uses a hierarchical configuration system with hot-reloading:

- **Config Location**: OS-specific paths via `dirs-next` (e.g., `~/.config/app_trans/config.json` on Linux)
- **Defaults**: Embedded in `defaults.json` at compile time
- **Structure**: Flat key-value pairs internally, nested JSON externally
- **Hot Reload**: File watcher (`notify` crate) automatically reloads config changes

**Key modules:**
- `app_config::manager::ConfigManager` - Main config interface with `defaults`, `user`, and `merged` layers
- `app_config::watcher` - File system watcher for hot-reload
- `app_config::value::ConfigValue` - Type-safe config value enum
- `app_config::schema::ConfigSchema` - Schema definitions with defaults

**Global access:**
- `init_system_config()` - Initialize config system (call once at startup)
- `get_config_manager()` - Get shared ConfigManager instance
- `get_system_config(task_id)` - Get Config for specific task

## Architecture

### Module Structure

```
src/
├── lib.rs                    # Public API, config initialization
├── core/                     # Core sync functionality
│   ├── mod.rs               # Config structs, HTTP server setup
│   ├── cli.rs               # CLI command definitions
│   ├── db.rs                # Database connection pooling (global pool cache)
│   ├── server.rs            # Sync logic, HTTP handlers
│   ├── axum_api.rs          # Axum route handlers
│   └── client_tool.rs       # HTTP client for API fetching
├── dsl_engine/              # DSL parser and executor
│   ├── mod.rs
│   └── syncengine.rs        # nom-based parser, AST, runtime
└── app_config/              # Configuration system
    ├── manager.rs           # ConfigManager with layered config
    ├── watcher.rs           # File watcher for hot-reload
    ├── value.rs             # ConfigValue enum
    ├── schema.rs            # ConfigSchema definitions
    ├── config_loader.rs     # JSON loading/flattening
    └── path.rs              # OS-specific config paths
```

### Data Flow

1. **Config Loading**: `init_system_config()` → loads `defaults.json` → merges with user config → starts watcher
2. **API Fetch**: `client_tool::fetch_json()` → HTTP request with headers/timeout
3. **Data Extraction**: `extract_by_path()` → JSONPath-style extraction (e.g., `/data/items` or `data.items`)
4. **Transformation**: DSL engine evaluates expressions like `upper(source.name)` or `concat(source.region, source.name)`
5. **Type Conversion**: `to_typed_value()` → converts JSON to typed SQL values (int/float/bool/timestamp/text)
6. **Database Sync**: Batch insert/upsert with transactions → uses global connection pool cache

### Database Connection Pooling

The `core::db` module maintains a **global connection pool cache** using `DashMap`:
- Pools are keyed by `(DbKind, url, max_conns, timeout)`
- Reuses existing pools for identical connection parameters
- Supports both PostgreSQL and MySQL via `sqlx`

### DSL Engine

The `dsl_engine::syncengine` module implements a compile-time validated DSL:

**Supported functions:**
- `upper(field)` - Convert to uppercase
- `concat(field1, field2)` - Concatenate strings
- `coalesce(field1, field2, ...)` - Return first non-null value

**Architecture:**
- **Parser**: nom-based parser converts DSL strings to AST
- **AST**: Enum-based (`DslOp`, `Expr`) for compile-time exhaustiveness checking
- **Runtime**: `SyncEngine::eval()` executes AST against JSON data

**Adding new functions:**
1. Add variant to `DslOp` enum (e.g., `Md5`)
2. Update `parse_op_type()` to recognize function name
3. Implement logic in `SyncEngine::eval()` match arm

### HTTP API Server

The server (`core::server.rs`) provides REST endpoints:

- `GET /tables?db_url=...&db_type=...&task_id=...` - List database tables
- `GET /describe?table=...&db_url=...` - Get table schema
- `GET /gen-mapping?table=...&db_url=...` - Generate column mapping template
- `POST /sync` - Execute sync with mapping config

**Task-based config resolution:**
- Query params can specify `task_id` to load config from ConfigManager
- Falls back to explicit `db_url`/`db_type` params if provided
- Enables remote control of sync tasks via HTTP

## Key Design Patterns

### Configuration Layers
The config system uses three layers (defaults → user → merged) to support:
- Embedded defaults from `defaults.json`
- User overrides in OS config directory
- Runtime registration of plugin configs
- Hot-reload without restart

### Global Singletons
Uses `OnceLock` for thread-safe lazy initialization:
- `SYSTEM_CONFIG` - Default task config
- `CONFIG_MANAGER` - Shared config manager
- `WATCHER_HOLDER` - File watcher handle
- `DB_POOLS` - Database connection pool cache

### Type-Safe SQL Binding
`TypedVal` enum ensures correct sqlx binding:
- Handles nullable vs non-nullable types
- Supports int/float/bool/timestamp/text
- Prevents SQL injection via parameterized queries

## Common Tasks

### Adding a New Sync Task

1. Edit config file (or use HTTP API):
```json
{
  "tasks": [
    {
      "id": "my_task",
      "db_type": "mysql",
      "db": {
        "url": "mysql://user:pass@localhost/db",
        "table": "target_table",
        "key_columns": ["id"]
      },
      "api": {
        "url": "https://api.example.com/data",
        "method": "GET",
        "headers": {"Authorization": "Bearer token"},
        "items_json_path": "/data/items"
      },
      "column_mapping": {
        "db_column": "api.field.path"
      },
      "column_types": {
        "db_column": "int"
      },
      "mode": "upsert",
      "batch_size": 100
    }
  ]
}
```

2. Access via `get_system_config(Some("my_task"))`

### Running Sync Programmatically

```rust
use data_trans_core::{init_system_config, get_system_config};
use data_trans_core::core::server::{get_pool_from_config, sync};

#[tokio::main]
async fn main() {
    init_system_config();
    let cfg = get_system_config(Some("my_task")).unwrap();
    let pool = get_pool_from_config(&cfg).await.unwrap();
    sync(&cfg, &pool).await.unwrap();
}
```

### Testing Config Changes

Use the examples to test config behavior:
```bash
cargo run --example test_config_watch      # Test hot-reload
cargo run --example test_persistence_flow  # Test config persistence
cargo run --example test_update_config     # Test config updates
```

## Dependencies

- **sqlx** (0.7) - Async SQL with compile-time query checking (MySQL/PostgreSQL)
- **tokio** - Async runtime (workspace-shared)
- **axum** (0.7) - HTTP server framework
- **nom** (7.1) - Parser combinator for DSL
- **serde/serde_json** - JSON serialization
- **notify** (6.1) - File system watcher
- **dashmap** (5) - Concurrent HashMap for connection pools
- **parking_lot** (0.12) - Efficient RwLock for config manager
- **flutter_rust_bridge** (2.11.1) - FFI bridge for Flutter integration

## Workspace Structure

This is part of a Cargo workspace:
- `data_trans_core` - Core library (this crate)
- `data_trans_cli` - CLI wrapper
- `my_app/rust` - Flutter desktop app integration

Shared dependencies are defined in workspace root `Cargo.toml`.
