use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use tracing::info;

// ---------------------------------------------------------------------------
// Anchor IDL data model (supports legacy + v0.30+ formats)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorIdl {
    #[serde(default)]
    pub address: Option<String>,
    pub metadata: IdlMetadata,
    pub instructions: Vec<IdlInstruction>,
    #[serde(default)]
    pub accounts: Vec<IdlAccountDef>,
    #[serde(default)]
    pub types: Vec<IdlTypeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlMetadata {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstruction {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
    #[serde(default)]
    pub accounts: Vec<IdlInstructionAccount>,
    #[serde(default)]
    pub args: Vec<IdlField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstructionAccount {
    pub name: String,
    #[serde(default)]
    pub writable: bool,
    #[serde(default)]
    pub signer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlAccountDef {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlTypeDef {
    pub name: String,
    #[serde(rename = "type")]
    pub type_def: IdlTypeDefTy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum IdlTypeDefTy {
    #[serde(rename = "struct")]
    Struct {
        #[serde(default)]
        fields: Vec<IdlField>,
    },
    #[serde(rename = "enum")]
    Enum {
        #[serde(default)]
        variants: Vec<IdlEnumVariant>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: IdlType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IdlType {
    Option { option: Box<IdlType> },
    Vec { vec: Box<IdlType> },
    Array { array: (Box<IdlType>, usize) },
    Defined { defined: IdlDefinedRef },
    Primitive(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlDefinedRef {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlEnumVariant {
    pub name: String,
    #[serde(default)]
    pub fields: Option<Vec<IdlField>>,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl AnchorIdl {
    /// Parse an Anchor IDL JSON file from disk.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read IDL at {path}: {e}"))?;
        let mut idl: Self = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse IDL JSON: {e}"))?;
        idl.backfill_discriminators();
        idl.log_summary();
        Ok(idl)
    }

    /// Fetch an IDL from an on-chain Anchor IDL account (zlib-compressed).
    pub async fn from_chain(
        rpc: &solana_client::nonblocking::rpc_client::RpcClient,
        idl_account: &solana_sdk::pubkey::Pubkey,
    ) -> anyhow::Result<Self> {
        use flate2::read::ZlibDecoder;

        info!(%idl_account, "Fetching IDL from on-chain account");

        let data = rpc
            .get_account_data(idl_account)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch IDL account: {e}"))?;

        // Anchor IDL account layout: [8B disc][32B authority][4B len][compressed...]
        anyhow::ensure!(data.len() > 44, "IDL account data too short");
        let data_len = u32::from_le_bytes(data[40..44].try_into()?) as usize;
        let compressed = &data[44..44 + data_len.min(data.len() - 44)];

        let mut decoder = ZlibDecoder::new(compressed);
        let mut json_str = String::new();
        decoder.read_to_string(&mut json_str)?;

        info!(bytes = json_str.len(), "IDL decompressed from chain");

        let mut idl: Self = serde_json::from_str(&json_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse on-chain IDL: {e}"))?;
        idl.backfill_discriminators();
        idl.log_summary();
        Ok(idl)
    }

    /// Build a lookup of custom type definitions by name.
    pub fn type_map(&self) -> HashMap<String, &IdlTypeDef> {
        self.types.iter().map(|t| (t.name.clone(), t)).collect()
    }

    // -- private helpers --

    fn backfill_discriminators(&mut self) {
        for ix in &mut self.instructions {
            if ix.discriminator.is_empty() {
                let pre = format!("global:{}", to_snake_case(&ix.name));
                ix.discriminator = sha256_first8(&pre);
            }
        }
        for acc in &mut self.accounts {
            if acc.discriminator.is_empty() {
                let pre = format!("account:{}", acc.name);
                acc.discriminator = sha256_first8(&pre);
            }
        }
    }

    fn log_summary(&self) {
        info!(
            name = %self.metadata.name,
            instructions = self.instructions.len(),
            accounts = self.accounts.len(),
            types = self.types.len(),
            "Anchor IDL loaded"
        );
    }
}

fn sha256_first8(input: &str) -> Vec<u8> {
    Sha256::digest(input.as_bytes())[..8].to_vec()
}

fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
