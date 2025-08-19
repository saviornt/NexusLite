# Bincode Version 2 Guide

## About bincode 2.0.1

- Release info: Version 2.0.1 of `bincode` was released on March 10, 2025.
- Dependencies and Features:
  - Optional dependency on `serde` - only enabled if you explicitly turn on the `serde` feature.
  - Default features includ `std` and `derive`.
  - The `serde` feature adds compatibility layers (`Compat` and `BorrowCompat`) and provides
  `serde`-specific functions in a `serde` submodule.

---

## New API

### `Encode` / `Decode`

Bincode 2 removes the `Serialize` and `Deserialize` traits and introduces its own traits:

- `Encode`: Converts Rust types to bytes with a binary serialization strategy.
- `Decode`: Converts bytes to Rust types using a binary deserialization strategy.

Both `Encode` and `Decode` are enabled by default using the `derive` feature.

### Key Functions

**Situation**                   | **Encode Function**     | **Decode Function**    |
--------------------------------|-------------------------|------------------------|
In-memory buffers               | `encode_to_vec`         | `decode_from_slice`    |
File or network streams         | `encode_into_std_write` | `decode_from_std_read` |
Custom writers/readers          | `encode_into_writer`    | `decode_from_reader`   |
Pre-allocated slices (embedded) | `encode_into_slice`     | `decode_from_slice`    |

If using `serde`, use the functions under the `bincode::serde::...` module instead.

## Working Examples

### Minimal Working Example

```Toml
# Cargo.toml

bincode = { version = "2.0.1", features = ["derive"]}
```

```Rust
// main.rs or lib.rs

use bincode::{config, Decode, Encode};

#[derive(Debug, PartialEq, Encode, Decode)]
struct Entity {
    x: f32,
    y: f32,
}

#[derive(Debug, PartialEq, Encode, Decode)]
struct World(Vec<Entity>);

fn main() {
    let cfg = config::standard();
    let world = World(vec![
        Entity { x: 0.0, y: 4.0 }
        Entity { x: 10.0, y: 20.5 },
    ]);
    
    let encoded: Vec<u8> = bincode::encode_to_vec(&world, cfg).unwrap();
    let (decoded, len): (World, usize) = bincode::decide_from_slice(&encoded, cfg).unwrap();

    assert_eq!(world, decoded);
    assert_eq!(len, encoded.len());
    println!("Encoded {} bytes, roundtrip succeeded!", len)
}
```

### Using the `serde` feature

```Toml
# Cargo.toml

bincode = { version = "2.0.1", features = ["derive", "serde"]}
```

```Rust
// main.rs or lib.rs

use bincode::serde::{encode_to_vec, decode_from_slice};
use serde::{Serialize, Deserialize}

#[derive(Serialize, Deserialize)]
struct type { ... }

// Use encode_to_vec and decode_from_slice from the `serde` submodule.
```
