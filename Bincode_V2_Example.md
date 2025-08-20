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

- `bincode::Encode`: Converts Rust types to bytes with a binary serialization strategy.
- `bincode::Decode`: Converts bytes to Rust types using a binary deserialization strategy.

Both `Encode` and `Decode` are enabled by default using the `derive` feature.

### `EncodeError` / `DecodeError`

Bincode 2 allows for proper error handling during (de)serialization by introducing dedicated error types:

- `bincode::error::EncodeError`: Represents errors that occur during encoding.
- `bincode::error::DecodeError`: Represents errors that occur during decoding.
- Both error types function with the `derive` and `serde` features, **there is no separate `bincode::serde::error` submodule.**

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
use bincode::error::{DecodeError, EncodeError};

#[derive(Debug, PartialEq, Encode, Decode)]
struct Entity {
    x: f32,
    y: f32,
}

#[derive(Debug, PartialEq, Encode, Decode)]
struct World(Vec<Entity>);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::standard();
    let world = World(vec![
        Entity { x: 0.0, y: 4.0 }
        Entity { x: 10.0, y: 20.5 },
    ]);

    let encoded: Vec<u8> = match bincode::encode_to_vec(&world, cfg) {
        Ok(vec) => vec,
        Err(e) => {
            match e {
                EncodeError::UnexpectedEnd => println!("Encoding failed: Unexpected end!"),
                other => println!("Encoding failed: {:?}", other),
            }
            return Err(Box::new(e));
        }
    };

    let (decoded, len): (World, usize) = match bincode::decide_from_slice(&encoded, cfg) {
        Ok(result) => result,
        Err(e) => {
            match e {
                DecodeError::UnexpectedEnd => println!("Decoding failed: Unexpected end!"),
                other => println!("Decoding failed: {:?}", other),
            }
            return Err(Box::new(e));
        }
    };

    assert_eq!(world, decoded);
    assert_eq!(len, encoded.len());
    println!("Encoded {} bytes, roundtrip succeeded!", len)
}
```

### Using the `serde` feature

```Toml
# Cargo.toml

bincode = { version = "2.0.1", features = ["derive", "serde"]}
serde = { version = "1.0.219", features = ["derive"] }
```

```Rust
// main.rs or lib.rs

use bincode::serde::{encode_to_vec, decode_from_slice};
use bincode::error::{EncodeError, DecodeError};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Point {
    x: i32,
    y: i32,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cfg = bincode::config::standard();
    let point = Point { x: 10, y: 20 };
    
    let encoded: Vec<u8> = match encode_to_vec(&point, cfg) {
        Ok(data) => data,
        Err(e) => e {
            EncodeError::UnexpectedEnd => println!("Encoding Error: Unexpected End!"),
            other => println!("Encoding Failed: {:?}", other),
        }
        return Err(Box::new(e));
    }

    let decoded: Point = match decode_from_slice(&encoded, cfg) {
        Ok(val) => data,
        Err(e) => e {
            DecodeError::UnexpectedEnd => println!("Decoding Error: Unexpected End!"),
            other => println!("Decoding Failed: {:?}", other),
        }
        return Err(Box::new(e));
    };

    println!("Encoded {} bytes", encoded.len());
    println!("Decoded point: {:?}", decoded);

    assert_eq!(point, decoded);
    Ok(())
}

```
