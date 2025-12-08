use std::fmt::Display;
use std::ops::Deref;

pub use near_indexer_primitives::near_primitives::serialize::dec_format;
use near_indexer_primitives::{
    types::{AccountId, BlockHeight},
    views::ExecutionStatusView,
    IndexerExecutionOutcomeWithReceipt,
};
use semver::Version;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

pub const MAINNET_GENESIS_BLOCK_HEIGHT: BlockHeight = 9820210;
pub const TESTNET_GENESIS_BLOCK_HEIGHT: BlockHeight = 100000000;

pub type FtBalance = u128;

/// Log data container that is used in [NEP-297](https://nomicon.io/Standards/EventsFormat).
#[derive(Deserialize, Debug, Clone)]
pub struct EventLogData<T> {
    pub standard: String,
    pub version: String,
    pub event: String,
    pub data: T,
}

impl<T> EventLogData<T> {
    pub fn parse_semver(&self) -> Result<Version, semver::Error> {
        Version::parse(&self.version)
    }

    /// Deserialize NEP-297 log data from JSON log string.
    ///
    /// If deserialization succeeds, you should still check [`EventLogData`] standard,
    /// event, and version fields to ensure that the log is actually relevant and is
    /// not a similar event that just happens to have the same fields.
    pub fn deserialize(log: &str) -> Result<EventLogData<T>, Nep297DeserializationError>
    where
        T: for<'de> Deserialize<'de>,
    {
        if let Some(log) = log.strip_prefix("EVENT_JSON:") {
            serde_json::from_str(log).map_err(Nep297DeserializationError::Deserialization)
        } else {
            Err(Nep297DeserializationError::NoPrefix)
        }
    }
}

#[derive(Debug)]
pub enum Nep297DeserializationError {
    Deserialization(serde_json::Error),
    NoPrefix,
}

impl Display for Nep297DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Nep297DeserializationError::Deserialization(e) => {
                write!(f, "Deserialization error: {}", e)
            }
            Nep297DeserializationError::NoPrefix => write!(f, "No 'EVENT_JSON:' prefix"),
        }
    }
}

#[test]
fn test_deserialize_nep297_log() {
    let log = r#"EVENT_JSON:{"standard":"nep171","version":"1.0.0","event":"nft_mint","data":[{"owner_id":"slimedragon.near","token_ids":["260"]}]}"#;
    let log_data: EventLogData<NftMintLog> = EventLogData::deserialize(log).unwrap();
    assert_eq!(log_data.standard, "nep171");
    assert_eq!(log_data.version, "1.0.0");
    assert_eq!(log_data.event, "nft_mint");
    assert_eq!(log_data.data[0].owner_id, "slimedragon.near");
    assert_eq!(log_data.data[0].token_ids, vec!["260"]);
    assert_eq!(log_data.data[0].memo, None);
}

pub const NEP141_EVENT_STANDARD_STRING: &str = "nep141";
pub const NEP171_EVENT_STANDARD_STRING: &str = "nep171";

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent, deny_unknown_fields)]
pub struct FtMintLog(pub Vec<FtMintEvent>);

impl Deref for FtMintLog {
    type Target = Vec<FtMintEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture tokens minting
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FtMintEvent {
    /// The account that minted the tokens
    pub owner_id: AccountId,
    /// The number of tokens minted
    #[serde(with = "dec_format")]
    pub amount: u128,
    /// Optional message
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent, deny_unknown_fields)]
pub struct FtBurnLog(pub Vec<FtBurnEvent>);

impl Deref for FtBurnLog {
    type Target = Vec<FtBurnEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture tokens burning
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FtBurnEvent {
    /// Owner of tokens to burn
    pub owner_id: AccountId,
    /// The number of tokens to burn
    #[serde(with = "dec_format")]
    pub amount: FtBalance,
    /// Optional message
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent, deny_unknown_fields)]
pub struct FtTransferLog(pub Vec<FtTransferEvent>);

impl Deref for FtTransferLog {
    type Target = Vec<FtTransferEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture tokens transfer
///
/// Note that some older tokens (including all `.tkn.near` tokens) don't follow this standard
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FtTransferEvent {
    /// The account ID of the old owner
    pub old_owner_id: AccountId,
    /// The account ID of the new owner
    pub new_owner_id: AccountId,
    /// The number of tokens to transfer
    #[serde(with = "dec_format")]
    pub amount: FtBalance,
    /// Optional message
    pub memo: Option<String>,
}

impl FtTransferLog {
    /// Deserialize this object from a string like "Transfer 250000000000000000000000 from slimedragon.near to intear.near"
    pub fn deserialize_tkn_farm_log(mut log: &str) -> Result<Self, String> {
        if !log.starts_with("Transfer ") {
            return Err("Log doesn't start with 'Transfer '".to_string());
        }
        log = log.trim_start_matches("Transfer ");
        let parts: Vec<&str> = log.split(" from ").collect();
        if parts.len() < 2 {
            return Err("Log doesn't contain ' from '".to_string());
        }
        if parts.len() > 2 {
            return Err("Log contains multiple ' from '".to_string());
        }
        let amount = parts[0]
            .parse::<FtBalance>()
            .map_err(|e| format!("Failed to parse transfer amount: {}", e))?;
        let parts: Vec<&str> = parts[1].split(" to ").collect();
        if parts.len() < 2 {
            return Err("Log doesn't contain ' to '".to_string());
        }
        if parts.len() > 2 {
            return Err("Log contains multiple ' to '".to_string());
        }
        let old_owner_id = parts[0]
            .parse()
            .map_err(|e| format!("Failed to parse old owner ID: {}", e))?;
        let new_owner_id = parts[1]
            .parse()
            .map_err(|e| format!("Failed to parse new owner ID: {}", e))?;
        Ok(Self(vec![FtTransferEvent {
            old_owner_id,
            new_owner_id,
            amount,
            memo: None,
        }]))
    }
}

#[test]
fn test_ft_transfer_log_deserialize_tkn_farm_log() {
    let log = "Transfer 250000000000000000000000 from slimedragon.near to intear.near";
    let transfer_log = FtTransferLog::deserialize_tkn_farm_log(log).unwrap();
    assert_eq!(transfer_log.len(), 1);
    assert_eq!(transfer_log[0].old_owner_id, "slimedragon.near");
    assert_eq!(transfer_log[0].new_owner_id, "intear.near");
    assert_eq!(transfer_log[0].amount, 250_000_000_000_000_000_000_000);
    assert_eq!(transfer_log[0].memo, None);
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(transparent, deny_unknown_fields)]
pub struct NftMintLog(pub Vec<NftMintEvent>);

impl Deref for NftMintLog {
    type Target = Vec<NftMintEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture token minting
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NftMintEvent {
    /// The account that minted the tokens
    pub owner_id: AccountId,
    /// The tokens minted
    pub token_ids: Vec<String>,
    /// Optional message
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(transparent, deny_unknown_fields)]
pub struct NftBurnLog(pub Vec<NftBurnEvent>);

impl Deref for NftBurnLog {
    type Target = Vec<NftBurnEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture token burning
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NftBurnEvent {
    /// Owner of tokens to burn
    pub owner_id: AccountId,
    /// Approved account_id to burn, if applicable
    pub authorized_id: Option<AccountId>,
    /// The tokens to burn
    pub token_ids: Vec<String>,
    /// Optional message
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(transparent, deny_unknown_fields)]
pub struct NftTransferLog(pub Vec<NftTransferEvent>);

impl Deref for NftTransferLog {
    type Target = Vec<NftTransferEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture token transfer
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NftTransferEvent {
    /// Approved account_id to transfer, if applicable
    pub authorized_id: Option<AccountId>,
    /// The account ID of the old owner
    pub old_owner_id: AccountId,
    /// The account ID of the new owner
    pub new_owner_id: AccountId,
    /// The tokens to transfer
    pub token_ids: Vec<String>,
    /// Optional message
    pub memo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(transparent, deny_unknown_fields)]
pub struct NftContractMetadataUpdateLog(pub Vec<NftContractMetadataUpdateEvent>);

impl Deref for NftContractMetadataUpdateLog {
    type Target = Vec<NftContractMetadataUpdateEvent>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An event log to capture contract metadata updates. Note that the updated contract metadata
/// is not included in the log, as it could easily exceed the 16KB log size limit. Listeners
/// can query `nft_metadata` to get the updated contract metadata.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NftContractMetadataUpdateEvent {
    /// Optional message
    pub memo: Option<String>,
}

impl EventLogData<FtMintLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP141_EVENT_STANDARD_STRING
                && self.event == "ft_mint"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_ft_mint_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep141","version":"1.0.0","event":"ft_mint","data":[{"owner_id":"slimedragon.near","amount":"250000000000000000000000"}]}"#;
    let log_data: EventLogData<FtMintLog> = EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

impl EventLogData<FtBurnLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP141_EVENT_STANDARD_STRING
                && self.event == "ft_burn"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_ft_burn_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep141","version":"1.0.0","event":"ft_burn","data":[{"owner_id":"slimedragon.near","amount":"250000000000000000000000"}]}"#;
    let log_data: EventLogData<FtBurnLog> = EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

impl EventLogData<FtTransferLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP141_EVENT_STANDARD_STRING
                && self.event == "ft_transfer"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_ft_transfer_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":[{"old_owner_id":"slimedragon.near","new_owner_id":"intear.near","amount":"250000000000000000000000"}]}"#;
    let log_data: EventLogData<FtTransferLog> = EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

impl EventLogData<NftMintLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP171_EVENT_STANDARD_STRING
                && self.event == "nft_mint"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_nft_mint_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep171","version":"1.0.0","event":"nft_mint","data":[{"owner_id":"slimedragon.near","token_ids":["260"]}]}"#;
    let log_data: EventLogData<NftMintLog> = EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

impl EventLogData<NftBurnLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP171_EVENT_STANDARD_STRING
                && self.event == "nft_burn"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_nft_burn_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep171","version":"1.0.0","event":"nft_burn","data":[{"owner_id":"slimedragon.near","token_ids":["260"]}]}"#;
    let log_data: EventLogData<NftBurnLog> = EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

impl EventLogData<NftTransferLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP171_EVENT_STANDARD_STRING
                && self.event == "nft_transfer"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_nft_transfer_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep171","version":"1.0.0","event":"nft_transfer","data":[{"old_owner_id":"slimedragon.near","new_owner_id":"intear.near","token_ids":["260"]}]}"#;
    let log_data: EventLogData<NftTransferLog> = EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

impl EventLogData<NftContractMetadataUpdateLog> {
    pub fn validate(&self) -> bool {
        if let Ok(version) = self.parse_semver() {
            self.standard == NEP171_EVENT_STANDARD_STRING
                && self.event == "contract_metadata_update"
                && version.major == 1
        } else {
            false
        }
    }
}

#[test]
fn test_nft_contract_metadata_update_log_validate() {
    let log = r#"EVENT_JSON:{"standard":"nep171","version":"1.0.0","event":"contract_metadata_update","data":[{}]}"#;
    let log_data: EventLogData<NftContractMetadataUpdateLog> =
        EventLogData::deserialize(log).unwrap();
    assert!(log_data.validate());
}

pub fn is_receipt_successful(receipt: &IndexerExecutionOutcomeWithReceipt) -> Option<bool> {
    match receipt.execution_outcome.outcome.status {
        ExecutionStatusView::Failure(_) => Some(false),
        ExecutionStatusView::SuccessValue(_) => Some(true),
        ExecutionStatusView::SuccessReceiptId(_) => Some(true),
        ExecutionStatusView::Unknown => None,
    }
}

pub mod dec_format_vec {
    use super::*;

    pub fn serialize<T, S>(value: &[T], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: dec_format::DecType,
    {
        serializer.collect_seq(value.iter().map(|item| item.serialize()))
    }

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum Value {
        U64(u64),
        String(String),
    }

    struct Visitor<T>(std::marker::PhantomData<T>);

    impl<'de, T> de::Visitor<'de> for Visitor<T>
    where
        T: dec_format::DecType,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut vec = Vec::<T>::new();
            while let Some(item) = seq.next_element::<Option<Value>>()? {
                vec.push(match item {
                    Some(Value::U64(value)) => T::from_u64(value),
                    Some(Value::String(value)) => {
                        T::try_from_str(&value).map_err(de::Error::custom)?
                    }
                    None => T::try_from_unit()
                        .map_err(|_| de::Error::invalid_type(de::Unexpected::Option, &self))?,
                });
            }
            Ok(vec)
        }
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: dec_format::DecType,
    {
        deserializer.deserialize_seq(Visitor(std::marker::PhantomData))
    }
}

#[test]
fn test_dec_format_vec() {
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(transparent)]
    struct TestVec {
        #[serde(with = "dec_format_vec")]
        vec: Vec<FtBalance>,
    }

    let test_vec = TestVec {
        vec: vec![1_000_000_000_000_000_000_000, 2_000_000_000_000_000_000_000],
    };
    let serialized = serde_json::to_string(&test_vec).unwrap();
    assert_eq!(
        serialized,
        r#"["1000000000000000000000","2000000000000000000000"]"#
    );
    let deserialized: TestVec = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, test_vec);
}

pub mod dec_format_map {
    use super::*;

    pub fn serialize<T, K, V, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        for<'a> &'a T: IntoIterator<Item = (&'a K, &'a V)>,
        K: Serialize,
        V: dec_format::DecType,
    {
        serializer.collect_map(value.into_iter().map(|(k, v)| (k, v.serialize())))
    }

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum Value {
        U64(u64),
        String(String),
    }

    struct Visitor<T, K, V>(
        std::marker::PhantomData<T>,
        std::marker::PhantomData<K>,
        std::marker::PhantomData<V>,
    );

    impl<'de, T, K, V> de::Visitor<'de> for Visitor<T, K, V>
    where
        K: Deserialize<'de>,
        V: dec_format::DecType,
        T: FromIterator<(K, V)>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            let mut items = Vec::with_capacity(map.size_hint().unwrap_or(0));
            while let Some((key, value)) = map.next_entry::<K, Value>()? {
                items.push((
                    key,
                    match value {
                        Value::U64(value) => V::from_u64(value),
                        Value::String(value) => {
                            V::try_from_str(&value).map_err(de::Error::custom)?
                        }
                    },
                ));
            }
            Ok(items.into_iter().collect())
        }
    }

    pub fn deserialize<'de, T, K, V, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: FromIterator<(K, V)>,
        K: Deserialize<'de>,
        V: dec_format::DecType,
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(Visitor(
            std::marker::PhantomData,
            std::marker::PhantomData,
            std::marker::PhantomData,
        ))
    }
}

#[test]
fn test_dec_format_map() {
    use std::collections::BTreeMap;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(transparent)]
    struct TestMap {
        #[serde(with = "dec_format_map")]
        map: BTreeMap<String, FtBalance>,
    }

    let mut map = BTreeMap::new();
    map.insert("one".to_string(), 1_000_000_000_000_000_000_000);
    map.insert("two".to_string(), 2_000_000_000_000_000_000_000);
    let test_map = TestMap { map };
    let serialized = serde_json::to_string(&test_map).unwrap();
    assert_eq!(
        serialized,
        r#"{"one":"1000000000000000000000","two":"2000000000000000000000"}"#
    );
    let deserialized: TestMap = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, test_map);
}
