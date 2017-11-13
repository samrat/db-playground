# external-sort

This is an implementation of external merge-sort ie. it sorts files
larger than what can fit in memory.

Currently, this only uses two input buffers. So technically, it is
what is called a *two phase, two-way merge sort*.

## Usage

```rust
ExternalMergeSort::sort_file("input_path", "sorted_output_path");
```
