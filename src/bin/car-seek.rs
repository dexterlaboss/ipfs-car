use anyhow::{Result, anyhow};
use std::env;
use dexter_ipfs_car::read_block_at_offset;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <car_file> <offset> <length>", args[0]);
        std::process::exit(1);
    }

    let car_path = &args[1];
    let offset: u64 = args[2].parse().map_err(|_| anyhow!("Invalid offset"))?;
    let length: u64 = args[3].parse().map_err(|_| anyhow!("Invalid length"))?;

    let (row_key, row_data) = read_block_at_offset(car_path, offset, length)?;

    println!("Row Key: {}", row_key);
    println!("Data: {}", String::from_utf8_lossy(&row_data));

    Ok(())
}