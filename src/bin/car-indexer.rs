use std::env;
use std::fs::File;
use std::io::BufReader;
use anyhow::Result;
use dexter_ipfs_car::generate_index_from_car_reader;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <car_file>", args[0]);
        std::process::exit(1);
    }

    let car_path = &args[1];

    // Open the CAR file using BufReader for efficient reading
    let file = File::open(car_path)?;
    let mut reader = BufReader::new(file);

    // Generate the index from the CAR reader
    let index = generate_index_from_car_reader(&mut reader)?;

    // Print the index
    for (row_key, offset, length) in index {
        println!("{} -> (offset={}, length={})", row_key, offset, length);
    }

    Ok(())
}