use anyhow::{Result, anyhow};
use std::env;
use std::fs::File;
use std::io::BufReader;
use dexter_ipfs_car::read_block_at_offset_reader;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <car_file> <offset> <length>", args[0]);
        std::process::exit(1);
    }

    let car_path = &args[1];
    let offset: u64 = args[2].parse().map_err(|_| anyhow!("Invalid offset"))?;
    let length: u64 = args[3].parse().map_err(|_| anyhow!("Invalid length"))?;

    // Open the CAR file using BufReader
    let file = File::open(car_path)?;
    let mut reader = BufReader::new(file);

    // Read the block at the specified offset and length
    let (row_key, row_data) = read_block_at_offset_reader(&mut reader, offset, length)?;

    // Print the row key and data
    println!("Row Key: {}", row_key);
    println!("Data: {}", String::from_utf8_lossy(&row_data));

    Ok(())
}