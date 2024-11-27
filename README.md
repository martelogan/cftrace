# Traceroute Data Collection Tool

This tool collects traceroute data from Cloudflare colos to a specified target IP or domain and organizes the results into structured JSON files and aggregate CSVs for analysis. It supports geo-location lookups, hop analysis, and optional GCP region identification for targets hosted on Google Cloud.

---

## **Directory Structure**
The results are saved in a structured directory hierarchy based on colos and regions:

- **Regions**: `na/` (North America), `emea/` (Europe, Middle East, and Africa), `apac/` (Asia-Pacific), `latam/` (Latin America).
- **Files**: Each file is named using a human-readable target name and the target IP, e.g., `c1-gclb_34.49.121.93.json`.

---

## **CSV Formats**

### **Traceroute Summary CSV**

The `traceroute_summary.csv` summarizes traceroute data for each `<colo, target>` pair. Below is the list of columns in the CSV, ordered as they appear:

| **Column Name**             | **Description**                                                                                      |
|-----------------------------|--------------------------------------------------------------------------------------------------|
| `start_region`              | Region grouping for the colo, e.g., `na`, `emea`, etc.                                           |
| `start_colo`                | The colo's short name, e.g., `pdx`.                                                              |
| `trace_target`              | Human-readable name for the target IP, e.g., `c1-gclb`.                                          |
| `rtt_ms`                    | Mean round-trip time across all hops in milliseconds (rounded to an integer).                    |
| `hops_count`                | Total number of hops in the traceroute.                                                          |
| `start_city`                | City, state, and country of the colo, e.g., `Portland, OR, US`.                                  |
| `approx_final_hop`          | Geo-location of the final hop, e.g., `Mountain View, CA, US`.                                    |
| `approx_nearest_gcp`        | Closest GCP region to the final hop, e.g., `us-east1`.                                           |
| `target_distance_km`        | Approximate distance (in kilometers) between the colo and the target. Defaults to `unknown`.     |
| `start_subcolo`             | The sub-colo (if provided in the API response), e.g., `pdx02`.                                   |
| `target_ip`                 | Target IP address, e.g., `34.49.121.93`.                                                         |
| `target_domain`             | Domain name of the target, e.g., `google.com`. Defaults to `unknown` if unavailable.             |
| `traceroute_time_ms`        | Total time for the traceroute in milliseconds.                                                   |
| `traceroute_packet_count`   | Total packets sent to the target.                                                                |
| `min_rtt_ms`                | Minimum RTT observed across all hops.                                                           |
| `max_rtt_ms`                | Maximum RTT observed across all hops.                                                           |
| `std_dev_rtt_ms`            | Standard deviation of RTTs across all hops.                                                     |
| `colo_lat`                  | Latitude of the colo. Defaults to `unknown` if unavailable.                                      |
| `colo_long`                 | Longitude of the colo. Defaults to `unknown` if unavailable.                                     |
| `colo_country`              | Country of the colo, e.g., `US`. Defaults to `unknown`.                                          |
| `target_lat`                | Latitude of the target. Defaults to `unknown` if unavailable.                                    |
| `target_long`               | Longitude of the target. Defaults to `unknown` if unavailable.                                   |
| `target_country`            | Country of the target, e.g., `US`. Defaults to `unknown`.                                        |

### **Skipped Colos CSV**

The `skipped_colos.csv` tracks colos that were skipped during data collection. Below is the list of columns:

| **Column Name**             | **Description**                                                                                  |
|-----------------------------|--------------------------------------------------------------------------------------------------|
| `start_region`              | Region grouping for the colo, e.g., `na`, `emea`, etc.                                          |
| `start_colo`                | The colo's short name, e.g., `pdx`.                                                             |
| `trace_target`              | Human-readable name for the target IP, e.g., `c1-gclb`.                                         |
| `target_ip`                 | Target IP address, e.g., `34.49.121.93`.                                                        |
| `target_domain`             | Domain name of the target, e.g., `google.com`. Defaults to `unknown` if unavailable.            |
| `skipped_reason`            | Reason the colo was skipped, e.g., `no_traceroute_response`.                                    |

---

## **Geo-Location and GCP Region Lookup**

### Geo-Location
- Geo-location data (`colo_lat`, `colo_long`, `colo_country`, `target_lat`, `target_long`, `target_country`) is fetched using a GeoIP API.
- Defaults to `unknown` if the API call fails or data is missing.

### GCP Region
- When the `target_is_gcp=true` flag is enabled, the `approx_nearest_gcp` column identifies the closest GCP region (e.g., `us-east1`) based on the final hop's geo-location.
- This is implemented using an in-memory lookup table mapping GCP regions to approximate lat/long coordinates.

---

## **Hop Analysis**

### Congested Hops
- Hops with **>50% packet loss** are flagged as congested.
- Stored in the JSON result files.

### Slowest Hops
- Hops with RTT >1 standard deviation above the mean are flagged as slow.
- Stored in the JSON result files.

---

## **Usage**

```bash
ruby traceroute_collector.rb --traceroute-uri URI --output-dir DIR --cf-colo-file FILE --targets IP:NAME:DOMAIN --region REGION
