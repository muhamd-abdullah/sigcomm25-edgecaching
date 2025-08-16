# Artifacts for *Edge Caching as Differentiation* (SIGCOMM ’25)

This repository contains the scripts and workflows used in our paper:  
**Edge Caching as Differentiation (SIGCOMM ’25)**.

Our goal is to measure **CDN caching behavior** (hit rates and latency) across different content providers, and to evaluate the impact on end-user **Quality of Experience (QoE)**. The repository provides end-to-end tools to:

1. **Crawl** content from video streaming platforms.  
2. **Measure** CDN hit rate and latency.  
3. **Simulate** video QoE under observed cache performance.

---

## Repository Structure

- [`crawler/`](./crawler)  
  A sample script to discover and collect URLs of CDN-hosted video segments from video steaming services. These URLs serve as inputs for measurements.  

- [`basic_measurement/`](./basic_measurement)  
  Tools to probe discovered URLs with lightweight **HEAD requests** and record per-request latencies, response headers, and cache hit/miss information.

- [`video_qoe/`](./video_qoe)  
  Modified [Sabre](https://github.com/UMass-LIDS/sabre) video player simulator, extended to replay measured latencies and compute QoE metrics.  

Each subfolder includes its own `README.md` with **setup, usage, and examples**.

---

## Workflow

1. **Crawl content URLs**  
   - Use `crawler/` example script to obtain video segment or object URLs for target platforms. 
   - Example: `crawler/example.py` for Vimeo.  

2. **Run basic measurements**  
   - Use `basic_measurement/main.py` to send HEAD requests to the collected URLs from your vantage point.  
   - Extract **latency** and **hit/miss** data from response headers.

3. **QoE computation**  
   - Pass the ouput from basic measurements into `video_qoe/run_sabre.py`.  
   - Simulate playback under different network conditions and compute **QoE metrics** (adjusted bitrate, startup delay, rebuffering, etc).  

---

## Notes on Reproducibility

- **CDN variability**: Cache hit rates and latency **will vary** with location and time of measurement.  
- To account for this, we provide **scripts rather than static datasets**, enabling others to repeat and extend our measurements.  

---

## Mapping to the Paper

The following table maps repository components to figures and analyses in the paper:

| Paper Section | Analysis / Plots | Repository Components |
|---------------|------------------|------------------------|
| **Section 3: Basic Measurement** | Latency and hit rate across vantage points | [`basic_measurement/`](./basic_measurement) and [`crawler/`](./crawler) |
| **Section 4: Video Quality** | QoE under different caching outcomes | [`video_qoe/`](./video_qoe) |
| **Section 5: Browsing Experience** | Social media browsing metrics (FCP, LCP) | [`basic_measurement/`](./basic_measurement) and [`crawler/`](./crawler) |

---

## Example End-to-End Usage

```bash
# 1. Crawl Vimeo URLs
cd crawler
python example.py --max-videos 10 --out-dir ../urls/vimeo

# 2. Measure latency and obtain HTTP responses
cd ../basic_measurement
python main.py ../urls/vimeo ./results --workers 20 --verbose

# 3. Process responses and obtain cache hit-rates
python helper.py ./results ./results/basic.parquet

# 4. Compute QoE with Sabre
cd ../video_qoe
python run_sabre.py \
  --input-parquet ../basic_measurement/results/basic.parquet \
  --output-parquet ./results/qoe.parquet \
  --bandwidth 25 \
  --miss-latency 370 \
  --buffer-size 30
```

---

## Citation

If you use these artifacts, please cite our paper:

```
@inproceedings{abdullah2025edgecaching,
  title={Edge Caching as Differentiation},
  author={Muhammad Abdullah, Mughees Ur Rehman, Pavlos Nikolopoulos, Katerina Argyraki},
  booktitle={ACM SIGCOMM 2025},
  year={2025}
}
```