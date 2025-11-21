# Lightning Search Benchmark Suite

This directory contains benchmarks for testing the stratified indexing system with datasets of various sizes.

## Quick Start

Run all benchmarks:
```powershell
.\run_benchmarks.ps1
```

Run specific benchmarks:
```powershell
# Small precision tests (~6 files, <1MB)
cargo test precision --quiet

# Medium dataset (~500 files, 100MB)
cargo test medium_dataset -- --ignored --nocapture

# Large dataset (~1000 files, 500MB)
cargo test large_dataset -- --ignored --nocapture
```

## Benchmark Configuration

### Medium Dataset
- **Files**: 500
- **Size per file**: ~200KB
- **Total size**: ~100MB
- **Groups**: 4
- **Sampling rate**: 1%

### Large Dataset
- **Files**: 1000
- **Size per file**: ~500KB
- **Total size**: ~500MB
- **Tests**:
  - 4 groups with 1% sampling
  - 8 groups with 1% sampling
  - 16 groups with 1% sampling

## What Gets Measured

Each benchmark measures:
1. **Indexing time**: How long it takes to build the stratified index
2. **Index size**: Total disk space used by the index
3. **Search performance**: Average query time across multiple search queries
4. **Result accuracy**: Number of results found per query

## Interpreting Results

### Good Performance Indicators
- **Indexing time** scales linearly with dataset size
- **Index size** is reasonable compared to source data (typically 2-5x)
- **Search time** remains low (< 1 second) even for large datasets
- **Group distribution** is relatively balanced (check stratification.json)

### What to Watch For
- Searches taking > 5 seconds may indicate poor stratification
- Index size > 10x source data suggests inefficiency
- Zero results means queries don't match generated content

## Generated Test Data

Test files contain repeating patterns from these base texts:
- "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
- "The quick brown fox jumps over the lazy dog."
- "Rust is a systems programming language focused on safety and performance."
- "Machine learning algorithms process vast amounts of data efficiently."
- "Database indexing improves query performance significantly."
- "Concurrent programming requires careful synchronization mechanisms."
- "Network protocols enable communication between distributed systems."
- "Data structures organize information for efficient access patterns."

Queries are designed to match these patterns.

## Output Structure

```
test_data/
  medium_dataset/          # Generated test files
  large_dataset/
test_output/
  medium_benchmark/        # Index output
    stratification.json    # Group boundaries
    group_0/              # Index files for group 0
    group_1/              # Index files for group 1
    ...
  large_benchmark_4g_1pct/
  large_benchmark_8g_1pct/
  large_benchmark_16g_1pct/
```

## Customizing Benchmarks

Edit `src/test/benchmark.rs` to adjust:
- Number of files
- File size
- Number of groups
- Sampling rate
- Query patterns

Example:
```rust
generate_large_dataset("./test_data/custom", 2000, 1000).await?;
benchmark_stratified("./test_data/custom", "./test_output/custom", 8, 0.02).await?;
```

## Performance Tips

1. **More groups** = more parallel search but higher overhead
2. **Higher sampling rate** = better stratification but slower indexing
3. **Larger files** = better compression but slower per-file processing
4. **SSD** recommended for test data and index output
