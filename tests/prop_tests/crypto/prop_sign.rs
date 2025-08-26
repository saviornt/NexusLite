use proptest::prelude::*;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        cases: 16,
        .. proptest::test_runner::Config::default()
    })]
    #[test]
    fn prop_p256_sign_verify_roundtrip_random_bytes(data in proptest::collection::vec(any::<u8>(), 0..1024)) {
        use nexuslite::crypto::signature_verification::ecdsa::{generate_p256_keypair_pem, sign_file_p256, verify_file_p256};
        use std::io::Write;
        let (priv_pem, pub_pem) = generate_p256_keypair_pem();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("msg.bin");
        {
            let mut f = std::fs::File::create(&p).unwrap();
            f.write_all(&data).unwrap();
        }
        let sig = sign_file_p256(&priv_pem, &p).unwrap();
        let ok = verify_file_p256(&pub_pem, &p, &sig).unwrap();
        prop_assert!(ok);
    }

    #[test]
    fn prop_p256_verify_rejects_tampered_data(mut data in proptest::collection::vec(any::<u8>(), 1..1024)) {
        use nexuslite::crypto::signature_verification::ecdsa::{generate_p256_keypair_pem, sign_file_p256, verify_file_p256};
        use std::io::Write;
        let (priv_pem, pub_pem) = generate_p256_keypair_pem();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("msg.bin");
        {
            let mut f = std::fs::File::create(&p).unwrap();
            f.write_all(&data).unwrap();
        }
        let sig = sign_file_p256(&priv_pem, &p).unwrap();
        // Tamper the file by flipping a bit
        data[0] ^= 0x01;
        std::fs::write(&p, &data).unwrap();
        let ok = verify_file_p256(&pub_pem, &p, &sig).unwrap();
        prop_assert!(!ok);
    }
}
