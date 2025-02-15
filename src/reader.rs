use std::io::{self, Read, Seek, SeekFrom, Cursor};
use anyhow::{Result, anyhow};
use cid::Cid;

use crate::{RowKey, RowData};
use crate::encoding::decode_row;

/// Reads **all rows** from an already-opened CAR reader.
///
/// The caller is responsible for providing an `R` that implements
/// `Read + Seek`, which could be:
/// - A `BufReader<File>` from the local filesystem
/// - A custom adapter that reads from HDFS
/// - An in-memory buffer, etc.
///
pub fn read_all_rows_from_car_reader<R: Read + Seek>(
    reader: &mut R
) -> Result<Vec<(RowKey, RowData)>> {
    // 1) Read the CAR header block
    let header_bytes = read_prefixed_block(reader)?;
    let (roots, _version) = parse_header(&header_bytes)?;

    if roots.is_empty() {
        return Err(anyhow!("No roots found in CAR file"));
    }

    // 2) For each root, read the next block and decode it
    let mut rows = Vec::new();
    for root_cid_str in &roots {
        let expected_cid: Cid = root_cid_str.parse()?;
        let block_bytes = read_prefixed_block(reader)?;
        let (cid, data) = split_cid_and_data(&block_bytes)?;

        if cid != expected_cid {
            return Err(anyhow!(
                "CID mismatch: expected {}, got {}",
                expected_cid, cid
            ));
        }

        // 3) Our CAR block contains a (row_key, row_data) that we decode:
        let (row_key, row_data) = decode_row(&data)?;
        rows.push((row_key, row_data));
    }

    Ok(rows)
}

/// Generates an index for **all blocks** in a CAR file (by reading them sequentially).
/// Returns `(RowKey, offset, length)` for each block.
///
/// The caller provides an `R` that implements `Read + Seek`.
pub fn generate_index_from_car_reader<R: Read + Seek>(
    reader: &mut R
) -> Result<Vec<(RowKey, u64, u64)>> {
    // Remember our start offset so we can compute block offsets
    let mut current_offset = reader.seek(SeekFrom::Current(0))?;

    // 1) Read CAR header
    let header_bytes = read_prefixed_block(reader)?;
    let (roots, _version) = parse_header(&header_bytes)?;
    if roots.is_empty() {
        return Err(anyhow!("No roots found in CAR file"));
    }

    // 2) After reading the header, `reader` is at the start of the first block
    let mut index = Vec::new();
    for _root_cid_str in &roots {
        // read_block_with_offset returns (offset, total_length, block_bytes)
        let (offset, length, block_bytes) = read_block_with_offset(reader, current_offset)?;
        let (_cid, data) = split_cid_and_data(&block_bytes)?;

        // Convert that data into (row_key, row_data)
        let (row_key, _row_data) = decode_row(&data)?;

        // Push it into our index
        index.push((row_key, offset, length));

        // Move offset forward
        current_offset += length;
    }

    Ok(index)
}

/// Reads a single block (RowKey, RowData) from an **already-opened** CAR stream,
/// but specifically at `offset` with size `length`. The offset+length should
/// include the varint prefix + CID + data, as recorded in an index.
///
/// Note that this function:
/// - Seeks `reader` to the given offset
/// - Reads exactly `length` bytes
/// - Parses the varint, extracts the CID, then extracts the row data
pub fn read_block_at_offset_reader<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    length: u64,
) -> Result<(RowKey, RowData)> {
    // 1) Seek to `offset`
    reader.seek(SeekFrom::Start(offset))?;

    // 2) Read exactly `length` bytes
    let mut block_buf = vec![0u8; length as usize];
    reader.read_exact(&mut block_buf)?;

    // 3) Within that buffer, the first part is a varint length
    //    that says how many bytes belong to the CID+data.
    let (cid, data) = split_cid_and_data_from_block(&block_buf)?;

    // 4) Decode row
    let (row_key, row_data) = decode_row(&data)?;
    Ok((row_key, row_data))
}


/// Reads a varint-prefixed block from the reader.
fn read_prefixed_block<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    let length = read_varint(r)?;
    let mut buf = vec![0; length as usize];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Reads a varint from the reader.
fn read_varint<R: Read>(r: &mut R) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        if r.read_exact(&mut byte).is_err() {
            return Err(anyhow!("Unexpected EOF reading varint"));
        }
        let b = byte[0];
        value |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            return Err(anyhow!("Varint too long"));
        }
    }
}

/// Reads a varint prefix for the upcoming CID+data, returning offset/length/block.
/// offset: where the block starts
/// length: total length (including varint prefix + CID+data)
fn read_block_with_offset<R: Read + Seek>(
    r: &mut R,
    start_offset: u64,
) -> Result<(u64, u64, Vec<u8>)> {
    let offset = start_offset;
    let length_value = read_varint(r)?; // how many bytes for CID+data
    let mut buf = vec![0; length_value as usize];
    r.read_exact(&mut buf)?;
    let end_offset = r.seek(SeekFrom::Current(0))?;
    let total_length = end_offset - offset;
    Ok((offset, total_length, buf))
}

/// Splits a block into (Cid, data) given `block` (already excludes the varint prefix).
fn split_cid_and_data(block: &[u8]) -> Result<(Cid, Vec<u8>)> {
    use std::io::Cursor;
    let mut cursor = Cursor::new(block);
    let cid = Cid::read_bytes(&mut cursor)?;
    let pos = cursor.position() as usize;
    let data = block[pos..].to_vec();
    Ok((cid, data))
}

/// Same as `split_cid_and_data`, but the input includes the varint prefix
/// plus the CID+data. We skip over the varint portion by reading it explicitly.
fn split_cid_and_data_from_block(block_with_prefix: &[u8]) -> Result<(Cid, Vec<u8>)> {
    let mut cursor = Cursor::new(block_with_prefix);
    let length = read_varint(&mut cursor)? as usize;
    let mut cid_data_buf = vec![0u8; length];
    cursor.read_exact(&mut cid_data_buf)?;
    split_cid_and_data(&cid_data_buf)
}

/// Parse the CAR header (CBOR-encoded {"roots": [...], "version": ... }).
fn parse_header(bytes: &[u8]) -> Result<(Vec<String>, u64)> {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct CarHeader {
        roots: Vec<String>,
        version: u64,
    }

    let ch: CarHeader = serde_cbor::from_slice(bytes)?;
    Ok((ch.roots, ch.version))
}