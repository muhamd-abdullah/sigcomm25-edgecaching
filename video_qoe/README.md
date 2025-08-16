# QoE for CDN-Delivered Videos

## Overview
This script runs a modified version of [**Sabre**](https://github.com/UMass-LIDS/sabre) video player simulator using multiple threads for faster processing at large scale. It is designed to evaluate the impact of CDN edge latency — including the option to set a fixed cache MISS latency — on video QoE under various network bandwidth conditions. 

Note: To see exactly what was changed in our version, compare our `src/sabre.py` with the original using `diff`.  

**Acknowledgement**: Thanks to the authors of Sabre for making their code open source, which this work builds upon. 

It supports **two modes**:
1. **Batch mode** (for large-scale anaylsis) – Reads a Parquet file containing per-video segment latencies and MISS indices (of segments not cached at the CDN edge), replaces MISS latencies by a fixed value (optional), runs Sabre in parallel across all videos, and saves results to a Parquet output.
2. **Direct mode** (for single run test) – Accepts segment latencies and MISS indices directly from the command line, runs Sabre once, and prints results.

---

## Requirements
- Python 3.8+
- Installed packages:
  - `pandas`
  - `pyarrow` (for Parquet I/O)

Install dependencies:
```bash
pip install pandas pyarrow
```

---

## Setup
1. **Navigate to your Sabre root**:
   ```bash
   cd /path/to/sabre
   ```

2. **Ensure video manifest file exist**:
   - `./config/bbb4k.json` — 4K Big Buck Bunny video manifest (segment sizes, bitrates, durations). We apply latencies to each video segment in this manifest based on the input.


3. **Prepare your input data** (for batch mode):
   - A Parquet file with `miss_indices` and `latency_list` columns (see below).

---

## Input Parquet Format


Each row in the input Parquet file corresponds to a single video and its associated segment data. It must include the following columns:
- `latency_list` — List of measured fetch latencies (ms) for each video segment.
- `miss_indices` — List of video segment indices (0-based) that were cache misses.

Example for a 5-segment video where segments at indices 2 and 1 were cache misses:
```text
latency_list = [25, 625, 700, 21, 19]
miss_indices = [2, 1]
```

If `--miss-latency 370` flag is specified when running the script (see [Example](#examples)), MISS latencies are normalized before the simulation:
```text
latency_list = [25, 370, 370, 21, 19]
```

Latencies of MISS segments are replaced with a fixed value (370 ms); rest of the segments remain unchanged.

---

## Examples

### Direct Mode
Run Sabre once with CLI-provided video segment data:
```bash
python run_sabre.py   --latency-list "[25,625,700,21,19]" --miss-indices "[2,1]" --bandwidth 25   --miss-latency 370   --buffer-size 30 --abr throughput   --verbose
```
- Useful for quick tests without creating a Parquet file.

### Batch Mode
Run Sabre on all rows from a Parquet file:
```bash
python run_sabre.py   --input-parquet ./data/example.parquet   --output-parquet ./result/output.parquet   --bandwidth 25   --miss-latency 62   --buffer-size 30   --max-workers 25
```
- Runs in parallel using `--max-workers` threads.
- Writes results to the specified output Parquet.

### Common Flags 
Use `--help` to see the list of flags at any time:  
```bash
python run_sabre.py --help
```
For the complete list of Sabre-specific flags, refer to the original [Sabre repository and documentation](https://github.com/UMass-LIDS/sabre).

| Flag | Type | Description |
|------|------|-------------|
| `--bandwidth` | float | Network bandwidth in Mbps (e.g., `25`, `100`). |
| `--miss-latency` | float | Latency (ms) to assign to cache MISS segments. |
| `--buffer-size` | int | Playback buffer size in seconds (default: `30`). |
| `--abr` | str | ABR algorithm to use (default: `dynamic`). |
| `--verbose` | flag | Enable verbose logging. In **Direct Mode**, verbose logging is always enabled. |

#### For Batch Mode (large-scale analysis)
| Flag | Type | Description |
|------|------|-------------|
| `--input-parquet` | str | Path to input Parquet file containing `miss_indices` and `latency_list` for each video. |
| `--output-parquet` | str | Path to save the results Parquet file. |
| `--max-workers` | int | Number of parallel worker threads to use (default: `25`). |

#### For Direct Mode (single run test)
| Flag | Type | Description |
|------|------|-------------|
| `--latency-list` | str | JSON list of per-segment latencies in ms (e.g., `"[25,625,700,21,19]"`). |
| `--miss-indices` | str | JSON list of cache MISS indices (e.g., `"[2,1]"`). |


---

## Output
Both modes produce:
- QoE metrics from Sabre’s output.
- The config parameters used in the run:
  - `sabre_miss_latency`
  - `sabre_bandwidth`
  - `sabre_buffer`
  - `sabre_abr` (default: `"dynamic"`)

In **batch mode**, results are saved to a Parquet file with one row per input video.  
In **direct mode**, results are printed to stdout (and optionally logged in detail with `--verbose`).

---

## Notes
- `--miss-latency` is inserted into the MISS segments of `latency_list` before simulation.
- For other video manifests, adjust `--manifest-json` accordingly to point to the desired manifest file.