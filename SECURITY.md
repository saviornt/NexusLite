# Security Policy

This project follows a security-first approach for code, dependencies, and release processes.

## Reporting a Vulnerability

If you discover a security vulnerability, please open a private issue or contact the maintainer directly. Avoid including sensitive details in public issues.

## Supply Chain and Code Hygiene

- Dependencies:
  - Use `cargo audit` and `cargo deny` in CI to fail on vulnerable, yanked, or unlicensed crates.
  - Prefer minimal, pinned dependency sets; avoid unnecessary transitive crates.
- Compiler and lints:
  - Build with warnings denied in CI and enable Clippy pedantic/nursery where possible.
  - Forbid `unsafe` in the crate (or gate behind a feature if absolutely required).
- Secrets and configs:
  - Do not store secrets in config files. Prefer environment variables and ensure logs redact secret-like keys.
- Reproducibility:
  - Keep lockfile committed. Avoid network access in build scripts.

## Cryptography

- ECC P-256 for encryption and signatures; Argon2id for password hashing.
- PBE (Argon2id → AES-256-GCM) for DB files, with TTY prompts masked.
- Future: hybrid PQC under feature flags.

## Hardening Roadmap (Sprint 6)

- CI: `cargo audit`, `cargo deny`, Clippy pedantic/nursery, rustfmt check.
- Forbid `unsafe` (crate-level) or gate behind feature.
- Document threat model and attack surface at a high level.
