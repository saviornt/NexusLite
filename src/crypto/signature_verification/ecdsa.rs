use p256::ecdsa::{
    Signature, SigningKey, VerifyingKey,
    signature::{Signer, Verifier},
};
use p256::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use std::fs;
use std::io::Read;

pub fn generate_p256_keypair_pem() -> (String, String) {
    use p256::elliptic_curve::rand_core::OsRng;
    let sk = SigningKey::random(&mut OsRng);
    let vk = sk.verifying_key();
    let priv_pem = sk.to_pkcs8_pem(Default::default()).expect("PEM encode").to_string();
    let pub_pem = vk.to_public_key_pem(Default::default()).expect("PEM encode");
    (priv_pem, pub_pem)
}

pub fn sign_file_p256(
    priv_pem: &str,
    input: &std::path::Path,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let sk = SigningKey::from_pkcs8_pem(priv_pem)?;
    let mut bytes = Vec::new();
    fs::File::open(input)?.read_to_end(&mut bytes)?;
    let sig: Signature = sk.sign(&bytes);
    Ok(sig.to_der().as_bytes().to_vec())
}

pub fn verify_file_p256(
    pub_pem: &str,
    input: &std::path::Path,
    sig_der: &[u8],
) -> Result<bool, Box<dyn std::error::Error>> {
    let vk = VerifyingKey::from_public_key_pem(pub_pem)?;
    let mut bytes = Vec::new();
    fs::File::open(input)?.read_to_end(&mut bytes)?;
    let sig = Signature::from_der(sig_der)?;
    Ok(vk.verify(&bytes, &sig).is_ok())
}
