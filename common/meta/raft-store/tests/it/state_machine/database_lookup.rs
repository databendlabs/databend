use common_meta_raft_store::state_machine::DatabaseLookupKey;
use common_meta_sled_store::SledOrderedSerde;

#[test]
fn test_db_lookup_key_serde() {
    {
        let k = DatabaseLookupKey::new("tenant1".to_string(), "db1".to_string());
        let ser_k = k.ser().unwrap();
        let de_k = DatabaseLookupKey::de(ser_k).unwrap();
        assert_eq!(k, de_k);
    }

    {
        let k = DatabaseLookupKey::new("tenant1".to_string(), "".to_string());
        let ser_k = k.ser().unwrap();
        let de_k = DatabaseLookupKey::de(ser_k).unwrap();
        assert_eq!(k, de_k);
    }
}
