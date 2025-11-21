use crate::core::index::{index_directory, IndexArgs};
use crate::core::search::{search_in_index_files, SearchArgs};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::sync::mpsc;

/// Generate a large test dataset
pub async fn generate_large_dataset(base_dir: &str, num_files: usize, file_size_kb: usize) -> std::io::Result<()> {
    let dir = Path::new(base_dir);
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)?;

    println!("Generating {} files of ~{}KB each...", num_files, file_size_kb);
    
    // Sample text to repeat and vary
    let base_texts = vec![
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ",
        "The quick brown fox jumps over the lazy dog. ",
        "Rust is a systems programming language focused on safety and performance. ",
        "Machine learning algorithms process vast amounts of data efficiently. ",
        "Database indexing improves query performance significantly. ",
        "Concurrent programming requires careful synchronization mechanisms. ",
        "Network protocols enable communication between distributed systems. ",
        "Data structures organize information for efficient access patterns. ",
    ];

    let target_size = file_size_kb * 1024;
    
    for i in 0..num_files {
        let file_path = dir.join(format!("file_{:06}.txt", i));
        let mut content = String::new();
        
        // Mix different base texts to create variety
        let text_idx = i % base_texts.len();
        while content.len() < target_size {
            content.push_str(base_texts[text_idx]);
            content.push_str(&format!(" [file_{} offset_{}] ", i, content.len()));
        }
        
        tokio::fs::write(&file_path, &content[..target_size]).await?;
        
        if (i + 1) % 100 == 0 {
            println!("  Generated {}/{} files", i + 1, num_files);
        }
    }
    
    println!("Dataset generation complete!");
    Ok(())
}

/// Run benchmark with stratified indexing
pub async fn benchmark_stratified(
    data_dir: &str,
    output_dir: &str,
    num_groups: usize,
    sampling_rate: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Stratified Indexing ===");
    println!("Data directory: {}", data_dir);
    println!("Output directory: {}", output_dir);
    println!("Number of groups: {}", num_groups);
    println!("Sampling rate: {:.2}%", sampling_rate * 100.0);
    
    // Clean output directory
    let output_path = Path::new(output_dir);
    if output_path.exists() {
        fs::remove_dir_all(output_path)?;
    }
    
    // Index phase
    let start = Instant::now();
    let args = IndexArgs {
        target_dir: data_dir.to_string(),
        includes: vec!["*".to_string()],
        excludes: vec![],
        read_chunk_size: 1024 * 1024 * 100,
        flush_threshold: 1024 * 1024 * 1024 * 100,
        channel_capacity: 320,
        output_dir: output_dir.to_string(),
        workers: 32,
        use_glob_cache: true,
        num_groups,
        sampling_rate,
    };
    
    let (sender, mut recvr) = mpsc::channel(10);
    let index_task = tokio::spawn(index_directory(args, sender));
    let _drain = tokio::spawn(async move {
        while let Some(_progress) = recvr.recv().await {
            // Consume progress updates
        }
    });
    
    index_task.await.expect("Index task panicked").expect("Index failed");
    let index_duration = start.elapsed();
    
    println!("\n--- Indexing Results ---");
    println!("Indexing time: {:.2}s", index_duration.as_secs_f64());
    
    // Calculate index size
    let index_size = calculate_dir_size(output_path)?;
    println!("Index size: {:.2}MB", index_size as f64 / (1024.0 * 1024.0));
    
    // Search phase - test multiple queries that will actually match the generated content
    let queries = vec![
        "Lorem ipsum dolor",
        "quick brown fox",
        "Rust is a systems",
        "Machine learning algorithms",
        "Database indexing improves",
    ];
    
    println!("\n--- Search Performance ---");
    let mut total_search_time = 0.0;
    let mut total_results = 0;
    
    for query in &queries {
        let start = Instant::now();
        let search_args = SearchArgs {
            index_dir: PathBuf::from(output_dir),
            workers: 32,
        };
        
        let (sender, mut recvr) = mpsc::channel(10);
        let search_task = tokio::spawn(search_in_index_files(
            search_args,
            query.as_bytes().to_vec(),
            sender,
        ));
        let _drain = tokio::spawn(async move {
            while let Some(_progress) = recvr.recv().await {
                // Consume progress updates
            }
        });
        
        let results = search_task.await.expect("Search task panicked").expect("Search failed");
        let search_duration = start.elapsed();
        
        total_search_time += search_duration.as_secs_f64();
        total_results += results.len();
        
        println!(
            "  Query: {:20} | Results: {:4} | Time: {:.4}s",
            format!("\"{}\"", query),
            results.len(),
            search_duration.as_secs_f64()
        );
    }
    
    println!("\nAverage search time: {:.4}s", total_search_time / queries.len() as f64);
    println!("Total results found: {}", total_results);
    
    Ok(())
}

/// Calculate total size of a directory recursively
fn calculate_dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total_size = 0u64;
    
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                total_size += calculate_dir_size(&path)?;
            } else {
                total_size += entry.metadata()?.len();
            }
        }
    }
    
    Ok(total_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Run with: cargo test large_dataset -- --ignored --nocapture
    async fn large_dataset_benchmark() {
        let data_dir = "./test_data/large_dataset";
        let output_dir = "./test_output/large_benchmark";
        
        // Generate ~500MB dataset (1000 files x 500KB each)
        generate_large_dataset(data_dir, 1000, 500)
            .await
            .expect("Failed to generate dataset");
        
        // Benchmark with different configurations
        println!("\n╔════════════════════════════════════════╗");
        println!("║  Stratified Indexing Benchmark        ║");
        println!("╚════════════════════════════════════════╝");
        
        // Test 1: 4 groups with 1% sampling
        benchmark_stratified(data_dir, &format!("{}_4g_1pct", output_dir), 4, 0.01)
            .await
            .expect("Benchmark failed");
        
        // Test 2: 8 groups with 1% sampling
        benchmark_stratified(data_dir, &format!("{}_8g_1pct", output_dir), 8, 0.01)
            .await
            .expect("Benchmark failed");
        
        // Test 3: 16 groups with 1% sampling
        benchmark_stratified(data_dir, &format!("{}_16g_1pct", output_dir), 16, 0.01)
            .await
            .expect("Benchmark failed");
        
        println!("\n✓ All benchmarks completed successfully!");
    }
    
    #[tokio::test]
    #[ignore] // Run with: cargo test medium_dataset -- --ignored --nocapture
    async fn medium_dataset_benchmark() {
        let data_dir = "./test_data/medium_dataset";
        let output_dir = "./test_output/medium_benchmark";
        
        // Generate ~100MB dataset (500 files x 200KB each)
        generate_large_dataset(data_dir, 500, 200)
            .await
            .expect("Failed to generate dataset");
        
        println!("\n╔════════════════════════════════════════╗");
        println!("║  Medium Dataset Benchmark             ║");
        println!("╚════════════════════════════════════════╝");
        
        benchmark_stratified(data_dir, output_dir, 4, 0.01)
            .await
            .expect("Benchmark failed");
        
        println!("\n✓ Benchmark completed successfully!");
    }
}
