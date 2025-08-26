# NexusLite

A lightweight, embeddable **NoSQL database engine** written in Rust. It is designed for **speed, reliability, and security**, NexusLite provides a document collection store with advanced recovery and cryptographic safeguards and can be used for both persistent and ephemeral documents.

---

## Features

- BSON document store with CRUD operations
- **API, CLI, and REPL** interfaces
- Import/Export using multiple formats (CSV, JSON, BSON)
- Configurable caching & indexing
- Security-first: Unsafe Rust checks, supply-chain validation and ECC-256
- Strong testing suite: Unit, Integration and property tests for CI/CD pipelines and extensive interactive tests for Mock testing.

[Explore full feature details -> Wiki](https://github.com/saviornt/NexusLite/wiki)

---

## Installation

Build source with Cargo:

```bash
cargo install nexuslite
```

Or clone and run locally:

```bash
git clone https://github.com/saviornt/NexusLite.git
cd NexusLite
cargo run
```

---

## Quick start

### Basic Database Operations with Persistent Documents

#### Rust API

```rust
// Create the example for basic CRUD operations including:
// - Create new database
// - Open the database
// - Create a new collection
// - Insert a persistent document
// - Read the persistent document
// - Update the persistent document
// - Delete the persistent document
// - Delete the collection
// - Close the database
```

#### CLI

```bash
# Create the CLI commands for basic database operations with persistent documents.
# The CLI commands for this example should follow the previous code example.
# Note: Examples should include the command syntax for Linux, Windows and MacOS
```

#### REPL

```bash
# Create the REPL commands for basic database operations with persistent documents.
```

### Basic Database Operations with Ephemeral Documents

```rust
// Create the example for basic CRUD operations including:
// - Open the database
// - Insert an ephemeral document
// - Read the ephemeral document from the collection
// - Update the ephemeral document
// - Delete the ephemeral document
// - Close the database
```

```bash
# Create the CLI commands for basic database operations with ephemeral documents.
# The CLI commands for this example should follow the previous code example.
# Note: Examples should include the command syntax for Linux, Windows and MacOS
```

```bash
# Create the REPL commands for basic database operations with ephemeral documents.
```

### Import/Export Operations

```rust
// Create the example for import/export operations including:
// - Open or create a database
// - Import a collection from NDJSON
// - Export the collection to CSV
// - Close the database
```

```bash
# Create the CLI commands for import/export operations with persistent documents.
# The CLI commands for this example should follow the previous code example.
# Note: Examples should include the command syntax for Linux, Windows and MacOS
```

```bash
# Create the REPL commands for import/export operations with persistent documents.
```

---

## Advanced Usage

- Feature flags (`crypto`, `logging`, `open-metrics`, `snapshot`, `regex`, `cli-bin`, `doctor`, `repl`)
- Recovery & Write-Ahead Shadow Paging (WASP)
- Logging and metrics
- Security, supply chain and PQC considerations

[See Advanced Topics -> Wiki](https://github.com/saviornt/NexusLite/wiki/Advanced-Topics)

---

## Development

- [Architecture](https://github.com/saviornt/NexusLite/wiki/Architecture)
- [Project Structure](https://github.com/saviornt/NexusLite/wiki/Project-Structure)
- [Testing Strategy](https://github.com/saviornt/NexusLite/wiki/Testing-Strategy)

---

## Security

- Unsafe Rust detection (`cargo-geiger`)
- Dependency auditing and license checks (`cargo-audit` & `cargo-deny`)
- Authentication and Verification using ECC-256 and ECDSA

[Security & Reliability -> Wiki](https://github.com/saviornt/NexusLite/wiki/Security-&-Reliability)

---

## Roadmap

- [x] Core CRUD and Recovery Engine
- [x] CLI & REPL
- [x] Advanced recovery and WASP tuning
- [x] Cryptographic operations
- [ ] Bindings (Python, WASM, ...)
- [ ] Experimental Features (PQC, Vector Maps)

[Project Roadmap](https://github.com/saviornt/NexusLite/wiki/Project-Roadmap)

---

## Contributing

Contributions are welcome!

Please see the [Contributing Guide](https://github.com/saviornt/NexusLite/wiki/Contributing-Guide) for coding standards, feature proposals, and security practices.

## License

MIT License, See [LICENSE](LICENSE) for details.

---

## Quick Links

- [Wiki Home](https://github.com/saviornt/NexusLite/wiki)
- [Architecture](https://github.com/saviornt/NexusLite/wiki/Architecture)
- [Advanced Usage](https://github.com/saviornt/NexusLite/wiki/Advanced-Usage)
- [Security](https://github.com/saviornt/NexusLite/wiki/Security-&-Reliability)

---
