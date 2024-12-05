use anyhow::Result;
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use serde::{Serialize, Deserialize};
use serde_cbor::{to_vec, from_slice};

use crate::{RowKey, RowData};

#[derive(Serialize, Deserialize)]
struct EncodedRow {
    key: RowKey,
    data: RowData,
}

/// Encodes `(RowKey, RowData)` into CBOR and returns `(Cid, Bytes)`.
pub fn encode_row(key: &RowKey, data: &RowData) -> Result<(Cid, Vec<u8>)> {
    let to_encode = EncodedRow {
        key: key.clone(),
        data: data.clone(),
    };
    let cbor_data = to_vec(&to_encode)?;
    let hash = Code::Sha2_256.digest(&cbor_data);
    let cid = Cid::new_v1(0x71, hash); // 0x71 = dag-cbor
    Ok((cid, cbor_data))
}

/// Decodes bytes (CBOR) into `(RowKey, RowData)`.
pub fn decode_row(bytes: &[u8]) -> Result<(RowKey, RowData)> {
    let decoded: EncodedRow = from_slice(bytes)?;
    Ok((decoded.key, decoded.data))
}