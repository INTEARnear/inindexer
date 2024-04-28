use std::fmt::Display;
use std::{ops::Deref, str::FromStr};

use near_indexer_primitives::{
    types::{AccountId, Balance, BlockHeight},
    views::ExecutionStatusView,
    IndexerExecutionOutcomeWithReceipt,
};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const MAINNET_GENESIS_BLOCK_HEIGHT: BlockHeight = 9820210;
pub const TESTNET_GENESIS_BLOCK_HEIGHT: BlockHeight = 100000000;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(transparent)]
pub struct StringifiedU128(String);

impl StringifiedU128 {
    pub fn deserialize_balance<'de, D>(deserializer: D) -> Result<Balance, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let stringified = StringifiedU128::deserialize(deserializer)?;
        stringified.0.parse().map_err(serde::de::Error::custom)
    }
}

impl ToString for StringifiedU128 {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}

pub type StringifiedBalance = StringifiedU128;

impl TryFrom<&StringifiedBalance> for Balance {
    type Error = <Balance as FromStr>::Err;

    fn try_from(value: &StringifiedBalance) -> Result<Self, Self::Error> {
        value.0.parse()
    }
}

impl From<Balance> for StringifiedBalance {
    fn from(value: Balance) -> Self {
        Self(value.to_string())
    }
}

#[test]
fn test_stringified_balance() {
    let balance = 250_000_000_000_000_000_000_000;
    let stringified_balance = StringifiedBalance::from(balance);
    assert_eq!(stringified_balance.0, "250000000000000000000000");
    let balance = Balance::try_from(&stringified_balance).unwrap();
    assert_eq!(balance, 250_000_000_000_000_000_000_000);
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
    #[serde(deserialize_with = "StringifiedBalance::deserialize_balance")]
    pub amount: Balance,
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
    #[serde(deserialize_with = "StringifiedBalance::deserialize_balance")]
    pub amount: Balance,
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
    #[serde(deserialize_with = "StringifiedBalance::deserialize_balance")]
    pub amount: Balance,
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
            .parse::<Balance>()
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
