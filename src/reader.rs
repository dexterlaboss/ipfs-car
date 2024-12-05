use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use anyhow::{Result, anyhow};
use cid::Cid;

use crate::{RowKey, RowData};
use crate::encoding::decode_row;

/// Reads all rows from a CAR file and returns `(RowKey, RowData)` for each root block.
pub fn read_all_rows_from_car<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<(RowKey, RowData)>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read the header
    let header_bytes = read_prefixed_block(&mut reader)?;
    let (roots, _version) = parse_header(&header_bytes)?;

    if roots.is_empty() {
        return Err(anyhow!("No roots found in CAR file"));
    }

    let mut rows = Vec::new();
    for root_cid_str in &roots {
        let expected_cid: Cid = root_cid_str.parse()?;
        let block_bytes = read_prefixed_block(&mut reader)?;
        let (cid, data) = split_cid_and_data(&block_bytes)?;

        if cid != expected_cid {
            return Err(anyhow!(
                "CID mismatch: expected {}, got {}",
                expected_cid, cid
            ));
        }

        let (row_key, row_data) = decode_row(&data)?;
        rows.push((row_key, row_data));
    }

    Ok(rows)
}

/// Generates an index for all blocks in a previously written CAR file.
/// The index is of the form `(RowKey, offset, length)` for each block.
pub fn generate_index_from_car<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<(RowKey, u64, u64)>> {
    let mut file = File::open(path)?;
    let mut reader = BufReader::new(&mut file);

    // Read and parse the header
    let _header_start = reader.seek(SeekFrom::Current(0))?;
    let header_bytes = read_prefixed_block(&mut reader)?;
    let (roots, _version) = parse_header(&header_bytes)?;
    if roots.is_empty() {
        return Err(anyhow!("No roots found in CAR file"));
    }

    // After reading the header, the reader is positioned at the start of the first block.
    let mut current_offset = reader.seek(SeekFrom::Current(0))?;
    let mut index = Vec::new();

    for _root_cid_str in &roots {
        let (offset, length, block_bytes) = read_block_with_offset(&mut reader, current_offset)?;
        let (_cid, data) = split_cid_and_data(&block_bytes)?;

        let (row_key, _row_data) = decode_row(&data)?;

        // Record the index entry
        index.push((row_key, offset, length));

        // Move the offset forward
        current_offset += length;
    }

    Ok(index)
}

/// Reads a varint-prefixed block and returns the offset, length, and block bytes.
/// offset: where the block (including varint prefix) started.
/// length: total length = varint prefix length + block bytes length.
fn read_block_with_offset<R: Read + Seek>(r: &mut R, start_offset: u64) -> Result<(u64, u64, Vec<u8>)> {
    let offset = start_offset;
    let length_value = read_varint(r)?; // length of CID+data
    let mut buf = vec![0; length_value as usize];
    r.read_exact(&mut buf)?;
    let end_offset = r.seek(SeekFrom::Current(0))?;
    let total_length = end_offset - offset;
    Ok((offset, total_length, buf))
}

/// Reads a single block at a given `offset` and `length` from the CAR file
/// without reading the entire file.
///
/// `offset` and `length` should point to the start and size of the block data
/// (including the varint length prefix and the block itself) as recorded by the index.
pub fn read_block_at_offset<P: AsRef<std::path::Path>>(
    path: P,
    offset: u64,
    length: u64,
) -> Result<(RowKey, RowData)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Seek to the offset
    reader.seek(SeekFrom::Start(offset))?;

    // Read the entire block (varint length + CID + data)
    let mut block_buf = vec![0u8; length as usize];
    reader.read_exact(&mut block_buf)?;

    // We know the block_buf starts with a varint length prefix followed by CID and data.
    // But since we already know `length`, we can skip parsing the varint again:
    // The varint prefix plus CID+data = block_buf.
    // The data block was previously constructed as:
    //    varint(length_of_cid_and_data), cid_bytes, data_bytes
    // We can reconstruct CID and data by parsing again.
    let (_cid, data) = split_cid_and_data_from_block(&block_buf)?;

    // Decode the row from data
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

/// Parse the CAR header (CBOR-encoded { "roots": [...], "version": 1 }).
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

/// Split the block into CID and data given a full block (without the varint prefix).
fn split_cid_and_data(block: &[u8]) -> Result<(Cid, Vec<u8>)> {
    use std::io::Cursor;
    let mut cursor = Cursor::new(block);
    let cid = Cid::read_bytes(&mut cursor)?;
    let pos = cursor.position() as usize;
    let data = block[pos..].to_vec();
    Ok((cid, data))
}

/// Similar to split_cid_and_data, but first skip the varint prefix.
fn split_cid_and_data_from_block(block_with_prefix: &[u8]) -> Result<(Cid, Vec<u8>)> {
    // The block_with_prefix includes:
    // varint(length_of_cid_and_data) + cid_bytes + data_bytes
    // We can read the varint to know how long cid+data is.
    let mut cursor = std::io::Cursor::new(block_with_prefix);
    let length = read_varint(&mut cursor)? as usize;
    let mut cid_data_buf = vec![0u8; length];
    cursor.read_exact(&mut cid_data_buf)?;
    split_cid_and_data(&cid_data_buf)
}