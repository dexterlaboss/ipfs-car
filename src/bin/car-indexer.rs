use std::env;
use anyhow::{Result};
use dexter_ipfs_car::generate_index_from_car;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <car_file>", args[0]);
        std::process::exit(1);
    }

    let car_path = &args[1];

    let index = generate_index_from_car(car_path)?;

    for (row_key, offset, length) in index {
        println!("{} -> (offset={}, length={})", row_key, offset, length);
    }

    Ok(())
}