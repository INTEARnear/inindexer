use std::str::FromStr;

use near_indexer_primitives::types::{AccountId, Balance, BlockHeight};
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

/// Deserialize log data from JSON log string.
///
/// NOTE: In most cases, you should wrap the type in [`EventLogData`], but this function
/// will also work if the log is not conventional, or is not prefixed with `EVENT_JSON:`.
///
/// If you are using NEP-297 logs, and deserialization succeeds, you should still check
/// [`EventLogData`] standard, event, and version fields to ensure that the log is actually
/// relevant and is not a similar event that just happens to have the same fields.
pub fn deserialize_json_log<T: for<'de> Deserialize<'de>>(
    mut log: &str,
) -> Result<T, serde_json::Error> {
    log = log.trim_start_matches("EVENT_JSON:");
    serde_json::from_str(log)
}

#[test]
fn test_deserialize_json_log() {
    let log = r#"{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":{"old_owner_id":"slimedragon.near","new_owner_id":"intear.near","amount":"250000000000000000000000"}}"#;
    let log_data: EventLogData<FtTransferLog> = deserialize_json_log(log).unwrap();
    assert_eq!(log_data.standard, "nep141");
    assert_eq!(log_data.version, "1.0.0");
    assert_eq!(log_data.event, "ft_transfer");
    assert_eq!(log_data.data.old_owner_id, "slimedragon.near");
    assert_eq!(log_data.data.new_owner_id, "intear.near");
    assert_eq!(log_data.data.amount, 250_000_000_000_000_000_000_000);
    assert_eq!(log_data.data.memo, None);
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

/// An event log to capture tokens minting
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FtMintLog {
    /// The account that minted the tokens
    pub owner_id: AccountId,
    /// The number of tokens minted
    #[serde(deserialize_with = "StringifiedBalance::deserialize_balance")]
    pub amount: Balance,
    /// Optional message
    pub memo: Option<String>,
}

/// An event log to capture tokens burning
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FtBurnLog {
    /// Owner of tokens to burn
    pub owner_id: AccountId,
    /// The number of tokens to burn
    #[serde(deserialize_with = "StringifiedBalance::deserialize_balance")]
    pub amount: Balance,
    /// Optional message
    pub memo: Option<String>,
}

/// An event log to capture tokens transfer
///
/// Note that some older tokens (including all `.tkn.near` tokens) don't follow this standard
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FtTransferLog {
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
        Ok(Self {
            old_owner_id,
            new_owner_id,
            amount,
            memo: None,
        })
    }
}

#[test]
fn test_ft_transfer_log_deserialize_tkn_farm_log() {
    let log = "Transfer 250000000000000000000000 from slimedragon.near to intear.near";
    let transfer_log = FtTransferLog::deserialize_tkn_farm_log(log).unwrap();
    assert_eq!(transfer_log.old_owner_id, "slimedragon.near");
    assert_eq!(transfer_log.new_owner_id, "intear.near");
    assert_eq!(transfer_log.amount, 250_000_000_000_000_000_000_000);
    assert_eq!(transfer_log.memo, None);
}

/// An event log to capture token minting
#[derive(Deserialize, Debug, Clone)]
pub struct NftMintLog {
    /// The account that minted the tokens
    pub owner_id: AccountId,
    /// The tokens minted
    pub token_ids: Vec<String>,
    /// Optional message
    pub memo: Option<String>,
}

/// An event log to capture token burning
#[derive(Deserialize, Debug, Clone)]
pub struct NftBurnLog {
    /// Owner of tokens to burn
    pub owner_id: AccountId,
    /// Approved account_id to burn, if applicable
    pub authorized_id: Option<AccountId>,
    /// The tokens to burn
    pub token_ids: Vec<String>,
    /// Optional message
    pub memo: Option<String>,
}

/// An event log to capture token transfer
#[derive(Deserialize, Debug, Clone)]
pub struct NftTransferLog {
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

/// An event log to capture contract metadata updates. Note that the updated contract metadata
/// is not included in the log, as it could easily exceed the 16KB log size limit. Listeners
/// can query `nft_metadata` to get the updated contract metadata.
#[derive(Deserialize, Debug, Clone)]
pub struct NftContractMetadataUpdateLog {
    /// Optional message
    pub memo: Option<String>,
}

impl<T> EventLogData<T> {
    pub fn parse_semver(&self) -> Result<Version, semver::Error> {
        Version::parse(&self.version)
    }
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
    let log = r#"{"standard":"nep141","version":"1.0.0","event":"ft_mint","data":{"owner_id":"slimedragon.near","amount":"250000000000000000000000"}}"#;
    let log_data: EventLogData<FtMintLog> = deserialize_json_log(log).unwrap();
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
    let log = r#"{"standard":"nep141","version":"1.0.0","event":"ft_burn","data":{"owner_id":"slimedragon.near","amount":"250000000000000000000000"}}"#;
    let log_data: EventLogData<FtBurnLog> = deserialize_json_log(log).unwrap();
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
    let log = r#"{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":{"old_owner_id":"slimedragon.near","new_owner_id":"intear.near","amount":"250000000000000000000000"}}"#;
    let log_data: EventLogData<FtTransferLog> = deserialize_json_log(log).unwrap();
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
    let log = r#"{"standard":"nep171","version":"1.0.0","event":"nft_mint","data":{"owner_id":"slimedragon.near","token_ids":["1","2","3"]}}"#;
    let log_data: EventLogData<NftMintLog> = deserialize_json_log(log).unwrap();
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
    let log = r#"{"standard":"nep171","version":"1.0.0","event":"nft_burn","data":{"owner_id":"slimedragon.near","token_ids":["1","2","3"]}}"#;
    let log_data: EventLogData<NftBurnLog> = deserialize_json_log(log).unwrap();
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
    let log = r#"{"standard":"nep171","version":"1.0.0","event":"nft_transfer","data":{"old_owner_id":"slimedragon.near","new_owner_id":"intear.near","token_ids":["1","2","3"]}}"#;
    let log_data: EventLogData<NftTransferLog> = deserialize_json_log(log).unwrap();
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
    let log =
        r#"{"standard":"nep171","version":"1.0.0","event":"contract_metadata_update","data":{}}"#;
    let log_data: EventLogData<NftContractMetadataUpdateLog> = deserialize_json_log(log).unwrap();
    assert!(log_data.validate());
}
