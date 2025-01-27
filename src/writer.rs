use std::fs::File;
use std::io::{Write, BufWriter, Seek, SeekFrom, Cursor};
use std::path::Path;
use anyhow::Result;
use cid::Cid;
use serde::Serialize;
use serde_cbor::to_vec;

use crate::{RowKey, RowData};
use crate::encoding::encode_row;

/// An index entry that maps a row_key to its (offset, length) in the CAR file.
#[derive(Debug)]
pub struct BlockIndexEntry {
    pub row_key: RowKey,
    pub offset: u64,
    pub length: u64,
}

/// CAR file writer
pub struct CarWriter {
    writer: BufWriter<File>,
    cids: Vec<(RowKey, Cid, Vec<u8>)>,
}

impl CarWriter {
    /// Create a new CarWriter that writes to the given path on local filesystem.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        Ok(CarWriter {
            writer: BufWriter::new(file),
            cids: Vec::new(),
        })
    }

    /// Add a row to the CAR file's internal buffer (not yet written).
    pub fn add_row(&mut self, key: &RowKey, data: &RowData) -> Result<()> {
        let (cid, block_data) = encode_row(key, data)?;
        self.cids.push((key.clone(), cid, block_data));
        Ok(())
    }

    /// Finalize and write the CAR file to disk.
    /// Returns an index of `(row_key, offset, length)` for each block.
    pub fn finalize(mut self) -> Result<Vec<BlockIndexEntry>> {
        self.write_header()?;

        let mut index = Vec::new();
        let mut current_offset = self.writer.seek(SeekFrom::Current(0))?;

        for (row_key, cid, block_data) in self.cids {
            // The start offset before writing this block
            let block_start_offset = current_offset;

            // Prepare block data: [CID bytes | row data]
            let mut block_buf = Vec::new();
            block_buf.extend_from_slice(&cid.to_bytes());
            block_buf.extend_from_slice(&block_data);

            // Write varint (block length) + block contents
            let length_bytes = write_varint_to_vec(block_buf.len() as u64);
            self.writer.write_all(&length_bytes)?;
            self.writer.write_all(&block_buf)?;

            let block_total_length = length_bytes.len() as u64 + block_buf.len() as u64;

            // Update offset
            current_offset += block_total_length;

            index.push(BlockIndexEntry {
                row_key,
                offset: block_start_offset,
                length: block_total_length,
            });
        }

        self.writer.flush()?;
        Ok(index)
    }

    fn write_header(&mut self) -> Result<()> {
        #[derive(Serialize)]
        struct CarHeader {
            roots: Vec<String>,
            version: u64,
        }

        // Each block is considered a "root" for simplicity
        let root_strings: Vec<String> =
            self.cids.iter().map(|(_, cid, _)| cid.to_string()).collect();

        let header = CarHeader {
            roots: root_strings,
            version: 1,
        };

        let header_bytes = to_vec(&header)?;
        let length_bytes = write_varint_to_vec(header_bytes.len() as u64);

        // varint(header_len) + header
        self.writer.write_all(&length_bytes)?;
        self.writer.write_all(&header_bytes)?;
        Ok(())
    }
}

/// Build CAR file in memory
pub struct InMemoryCarBuilder {
    buffer: Cursor<Vec<u8>>,
    cids: Vec<(RowKey, Cid, Vec<u8>)>,
}

impl InMemoryCarBuilder {
    /// Create an in-memory CarBuilder that writes to a buffer (Vec<u8>).
    pub fn new() -> Self {
        InMemoryCarBuilder {
            buffer: Cursor::new(Vec::new()),
            cids: Vec::new(),
        }
    }

    /// Add a row to the in-memory buffer (not yet written).
    pub fn add_row(&mut self, key: &RowKey, data: &RowData) -> Result<()> {
        let (cid, block_data) = encode_row(key, data)?;
        self.cids.push((key.clone(), cid, block_data));
        Ok(())
    }

    /// Finalize and build the CAR file in memory.
    /// Returns `(car_bytes, index)`.
    pub fn finalize(mut self) -> Result<(Vec<u8>, Vec<BlockIndexEntry>)> {
        self.write_header()?;

        let mut index = Vec::new();
        let mut current_offset = self.buffer.seek(SeekFrom::Current(0))?;

        for (row_key, cid, block_data) in self.cids {
            let block_start_offset = current_offset;

            // Build the block
            let mut block_buf = Vec::new();
            block_buf.extend_from_slice(&cid.to_bytes());
            block_buf.extend_from_slice(&block_data);

            // varint length
            let length_bytes = write_varint_to_vec(block_buf.len() as u64);
            self.buffer.write_all(&length_bytes)?;
            self.buffer.write_all(&block_buf)?;

            let block_total_length = length_bytes.len() as u64 + block_buf.len() as u64;
            current_offset += block_total_length;

            index.push(BlockIndexEntry {
                row_key,
                offset: block_start_offset,
                length: block_total_length,
            });
        }

        self.buffer.flush()?;
        let final_data = self.buffer.into_inner();
        Ok((final_data, index))
    }

    fn write_header(&mut self) -> Result<()> {
        #[derive(Serialize)]
        struct CarHeader {
            roots: Vec<String>,
            version: u64,
        }

        let root_strings: Vec<String> =
            self.cids.iter().map(|(_, cid, _)| cid.to_string()).collect();

        let header = CarHeader {
            roots: root_strings,
            version: 1,
        };

        let header_bytes = to_vec(&header)?;
        let length_bytes = write_varint_to_vec(header_bytes.len() as u64);

        // varint(header_len) + header
        self.buffer.write_all(&length_bytes)?;
        self.buffer.write_all(&header_bytes)?;
        Ok(())
    }
}


/// A convenience function for writing multiple rows directly to a file.
pub fn write_multiple_rows_as_car<P: AsRef<std::path::Path>>(
    path: P,
    rows: &[(RowKey, RowData)],
) -> Result<Vec<BlockIndexEntry>> {
    let mut writer = CarWriter::new(path)?;
    for (key, data) in rows {
        writer.add_row(key, data)?;
    }
    let index = writer.finalize()?;
    Ok(index)
}

/// A convenience function for building a CAR file entirely in memory
pub fn build_in_memory_car(
    rows: &[(RowKey, RowData)],
) -> Result<(Vec<u8>, Vec<BlockIndexEntry>)> {
    let mut builder = InMemoryCarBuilder::new();
    for (key, data) in rows {
        builder.add_row(key, data)?;
    }
    let (car_bytes, index) = builder.finalize()?;
    Ok((car_bytes, index))
}

/// Encode a 64-bit value into a varint stored in a `Vec<u8>`.
fn write_varint_to_vec(mut value: u64) -> Vec<u8> {
    let mut buf = [0u8; 10];
    let mut i = 0;
    while value >= 0x80 {
        buf[i] = ((value & 0x7F) as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    i += 1;
    buf[..i].to_vec()
}