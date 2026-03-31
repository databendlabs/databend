// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Keys used for table encryption
///
/// Serializing of `encrypted_key_metadata` is done using base64 encoding.
#[derive(Debug, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct EncryptedKey {
    /// Unique identifier for the key
    #[builder(setter(into))]
    pub(crate) key_id: String,
    /// Encrypted key metadata as binary data
    #[builder(setter(into))]
    pub(crate) encrypted_key_metadata: Vec<u8>,
    /// Identifier of the entity that encrypted this key
    #[builder(default, setter(into, strip_option))]
    pub(crate) encrypted_by_id: Option<String>,
    /// Additional properties associated with the key
    #[builder(default)]
    pub(crate) properties: HashMap<String, String>,
}

impl EncryptedKey {
    /// Returns the key ID
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Returns the encrypted key metadata
    pub fn encrypted_key_metadata(&self) -> &[u8] {
        &self.encrypted_key_metadata
    }

    /// Returns the ID of the entity that encrypted this key
    pub fn encrypted_by_id(&self) -> Option<&str> {
        self.encrypted_by_id.as_deref()
    }

    /// Returns the properties map
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

pub(super) mod _serde {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;

    use super::*;

    /// Helper struct for serializing/deserializing EncryptedKey
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub(super) struct EncryptedKeySerde {
        pub key_id: String,
        pub encrypted_key_metadata: String, // Base64 encoded
        pub encrypted_by_id: Option<String>,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        pub properties: HashMap<String, String>,
    }

    impl From<&EncryptedKey> for EncryptedKeySerde {
        fn from(key: &EncryptedKey) -> Self {
            Self {
                key_id: key.key_id.clone(),
                encrypted_key_metadata: BASE64.encode(&key.encrypted_key_metadata),
                encrypted_by_id: key.encrypted_by_id.clone(),
                properties: key.properties.clone(),
            }
        }
    }

    impl TryFrom<EncryptedKeySerde> for EncryptedKey {
        type Error = base64::DecodeError;

        fn try_from(serde_key: EncryptedKeySerde) -> Result<Self, Self::Error> {
            let encrypted_key_metadata = BASE64.decode(&serde_key.encrypted_key_metadata)?;

            Ok(Self {
                key_id: serde_key.key_id,
                encrypted_key_metadata,
                encrypted_by_id: serde_key.encrypted_by_id,
                properties: serde_key.properties,
            })
        }
    }
}

impl Serialize for EncryptedKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let serde_key = _serde::EncryptedKeySerde::from(self);
        serde_key.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EncryptedKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let serde_key = _serde::EncryptedKeySerde::deserialize(deserializer)?;

        Self::try_from(serde_key).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_encrypted_key_serialization() {
        // Test data
        let metadata = b"iceberg";
        let mut properties = HashMap::new();
        properties.insert("algo".to_string(), "AES-256".to_string());
        properties.insert("created-at".to_string(), "2023-05-15T10:30:00Z".to_string());

        // Create the encrypted key
        let key = EncryptedKey::builder()
            .key_id("5f819b")
            .encrypted_key_metadata(metadata.to_vec())
            .encrypted_by_id("user-456")
            .properties(properties)
            .build();

        // Serialize to JSON
        let serialized = serde_json::to_value(&key).unwrap();

        let expected = json!({
            "key-id": "5f819b",
            "encrypted-key-metadata": "aWNlYmVyZw==",
            "encrypted-by-id": "user-456",
            "properties": {
                "algo": "AES-256",
                "created-at": "2023-05-15T10:30:00Z"
            }
        });
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_encrypted_key_round_trip() {
        // Test data
        let metadata = b"binary\0data\xff\xfe with special bytes";
        let mut properties = HashMap::new();
        properties.insert("algo".to_string(), "AES-256".to_string());

        // Create the original encrypted key
        let original_key = EncryptedKey::builder()
            .key_id("key-abc")
            .encrypted_key_metadata(metadata.to_vec())
            .encrypted_by_id("service-xyz")
            .properties(properties)
            .build();

        // Serialize to JSON string
        let json_string = serde_json::to_string(&original_key).unwrap();

        // Deserialize back from JSON string
        let deserialized_key: EncryptedKey = serde_json::from_str(&json_string).unwrap();

        // Verify the keys match
        assert_eq!(deserialized_key, original_key);
        assert_eq!(deserialized_key.encrypted_key_metadata(), metadata);
    }

    #[test]
    fn test_encrypted_key_empty_properties() {
        // Create a key without properties
        let key = EncryptedKey::builder()
            .key_id("key-123")
            .encrypted_key_metadata(b"data".to_vec())
            .encrypted_by_id("user-456")
            .build();

        // Serialize to JSON
        let serialized = serde_json::to_value(&key).unwrap();

        // Verify properties field is skipped when empty
        assert!(!serialized.as_object().unwrap().contains_key("properties"));

        // Deserialize back
        let deserialized: EncryptedKey = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized.properties().len(), 0);
    }

    #[test]
    fn test_invalid_base64() {
        // Invalid base64 string
        let json_value = json!({
            "key-id": "key-123",
            "encrypted-key-metadata": "invalid@base64",
            "encrypted-by-id": "user-456"
        });

        // Attempt to deserialize should fail
        let result: Result<EncryptedKey, _> = serde_json::from_value(json_value);
        assert!(result.is_err());
    }
}
