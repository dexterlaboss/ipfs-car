use dexter_ipfs_car::read_all_rows_from_car_reader;
use std::env;
use std::fs::File;
use std::io::BufReader;
use anyhow::Result;

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

    let read_rows = read_all_rows_from_car_reader(&mut reader)?;

    for (key, data) in read_rows {
        println!(
            "Read row: key = '{}', data = '{}'",
            key,
            String::from_utf8_lossy(&data)
        );
    }

    Ok(())
}