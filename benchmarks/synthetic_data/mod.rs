use csv::Writer;
use fake::{Fake, faker::name::en::Name};
use rand::Rng;
use std::path::Path;

fn generate_csv_chunked(path: &str, total_rows: usize, chunk_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = Writer::from_path(path)?;
    let mut rng = rand::thread_rng();

    // Write header
    wtr.write_record(&["id", "name", "value", "flag"])?;

    let mut id_counter = 0;
    while id_counter < total_rows {
        let end = std::cmp::min(id_counter + chunk_size, total_rows);
        for i in id_counter..end {
            let name: String = Name().fake();
            let value: f64 = rng.gen_range(0.0..10000.0);
            let flag: bool = rng.gen();
            wtr.write_record(&[i.to_string(), name, value.to_string(), flag.to_string()])?;
        }
        wtr.flush()?; // flush each chunk to disk
        id_counter += chunk_size;
        println!("Generated rows: {}/{}", id_counter, total_rows);
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let csv_path = "synthetic_data.csv";

    if !Path::new(csv_path).exists() {
        println!("File not found. Generating synthetic data in chunks...");
        generate_csv_chunked(csv_path, 1_000_000, 100_000)?; // 1M rows, 100k per chunk, ~100mb total
        println!("Synthetic data generated at '{}'.", csv_path);
    } else {
        println!("File '{}' already exists. Skipping generation.", csv_path);
    }

    Ok(())
}
