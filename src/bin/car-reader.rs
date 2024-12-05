use dexter_ipfs_car::{read_all_rows_from_car};
use std::env;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <car_file>", args[0]);
        std::process::exit(1);
    }

    let car_path = &args[1];

    let read_rows = read_all_rows_from_car(car_path)?;
    for (key, data) in read_rows {
        println!("Read row: key = '{}', data = '{}'", key, String::from_utf8_lossy(&data));
    }

    Ok(())
}