mod synthetic_data;

fn main() {
    // Ensure synthetic CSV exists for benchmarking purposes.
    let csv_path = synthetic_data::ensure_synthetic_data("benchmarks/synthetic_data/synthetic_data.csv")
        .expect("failed to prepare synthetic benchmark data");
    println!("Synthetic data ready at {:?}", csv_path);

    // Placeholder: real benchmark routines would go here and use `csv_path`.
}
