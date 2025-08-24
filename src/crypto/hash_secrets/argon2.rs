use p256::elliptic_curve::rand_core::{OsRng, RngCore};

/// Hash the specified top-level fields in a BSON document using Argon2id.
pub fn hash_secret_fields(
    doc: &mut bson::Document,
    fields: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let salt: [u8; 16] = {
        let mut s = [0u8; 16];
        OsRng.fill_bytes(&mut s);
        s
    };
    let argon = argon2::Argon2::default();
    for &field in fields {
        if let Some(bson::Bson::String(s)) = doc.get(field) {
            let mut out = [0u8; 32];
            argon
                .hash_password_into(s.as_bytes(), &salt, &mut out)
                .map_err(|e| std::io::Error::other(format!("argon2: {e}")))?;
            doc.insert(
                field,
                bson::Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: out.to_vec(),
                }),
            );
        }
    }
    Ok(())
}
