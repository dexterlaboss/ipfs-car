use dexter_ipfs_car::{write_multiple_rows_as_car};
use std::env;
use std::io::{self, BufRead};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <car_file>", args[0]);
        std::process::exit(1);
    }

    let car_path = &args[1];
    let mut rows = Vec::new();

    println!("Enter car entries in the format `<row_key> <data>`, one per line (Ctrl+D to finish):");

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if let Some((key, data)) = line.split_once(' ') {
            rows.push((key.to_string(), data.as_bytes().to_vec()));
        } else {
            eprintln!("Invalid format: `{}`. Expected `<row_key> <data>`.", line);
        }
    }

    if rows.is_empty() {
        eprintln!("No valid entries provided.");
        std::process::exit(1);
    }

    write_multiple_rows_as_car(car_path, &rows)?;

    println!("Done writing {}", car_path);

    Ok(())
}