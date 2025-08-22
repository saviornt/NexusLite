# NexusLite Threat Model (v0.1)

Scope

- Assets: user data (documents), keys and passwords, WASP manifest/segments, logs, config.
- Actors: user/admin, local attacker on host, malicious dependency, CI actor, filesystem adversary.
- Trust boundaries: CLI/API ↔ engine ↔ filesystem; human TTY vs non-interactive env; dependency supply chain.

Assumptions

- Single-process embedded DB on a trusted OS user account.
- No network exposure; all I/O is local files.
- Users can opt-in to PBE encryption and signature verification.

Key risks and mitigations

- Credential exposure: masked TTY prompts; env-only for non-interactive; redact secrets in logs.
- Tampered DB/WASP: optional signature verification with policy and override; snapshot embeds index descriptors.
- Data-at-rest: PBE AES-256-GCM with Argon2id; authenticated encryption; header format versioned.
- Supply chain: CI runs audit/deny; Dependabot weekly; forbid unsafe; clippy pedantic; license and source policies.
- Logging: logs written next to DB; paths sanitized; secret redaction enforced.

Out of scope (for now)

- Multi-tenant isolation; remote protocol; hardware enclave; formal crypto proofs.

Validation

- Tests cover PBE open/restore, tamper detection, and CLI flows; CI enforces lint/security checks.

Future

- Migrate all lazy statics to std LazyLock (done in core/test paths); extend to any remaining modules.
- Add file permissions hardening on Unix/Windows; explicit secure wipe for temp material; structured logs.
