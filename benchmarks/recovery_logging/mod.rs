// This is where we load all of the various recovery logging engines and then bookmark them
// Results should be logged and appended to `benchmarks/results/recovery_logging_{date}.csv`

// We should create and save a data structure using 100mb worth of synthetic data and then store it as a csv.
// The synthetic data can be saved to `/benchmarks/synthetic_data/`
// This synthetic data is what we use to test each of the recovery logging systems.

// To generate the synthetic data, use a combination of the `fake` and `csv` crate to generate the data and save it.