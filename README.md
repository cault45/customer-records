# Customer Transaction Data Pipeline

A production quality Python data pipeline that ingests customer and transaction data from REST APIs, validates and cleans the data, segments customers by transaction activity, and produces summary outputs for an analytics team.

---

## Project Overview

This pipeline was built as a take home data engineering task simulating a real world fintech use case. It ingests customer and transaction records from two API endpoints, applies validation and cleaning rules, joins and transforms the data, and produces three outputs — a cleaned parquet file, a city level summary CSV, and a rejected records JSON for auditing.

The pipeline is designed to be production quality with proper error handling, logging, config driven execution and clear separation of concerns across multiple modules.

---

## Pipeline Architecture

```
API Endpoints
      ↓
  pull_api()          # Generic API fetch with error handling
      ↓
   clean()            # Validate, standardise, track rejections
      ↓
  transform()         # Join customers to transactions, derive segments
      ↓
  summarise()         # Aggregate by city
      ↓
 save_outputs()       # Write parquet, CSV and JSON outputs
```

### Validation Rules

Records are rejected and written to `rejected.json` if they fail any of the following:

| Rule | Description |
|---|---|
| Null key fields | `id`, `name` or `email` are null |
| Invalid email | Email does not contain `@` or `.` |

Transaction records are validated for null `id` and `userId` — rejected counts are logged but transactions are not written to a separate output file.

### Customer Segmentation

Customers are segmented based on total transaction count:

| Segment | Transactions |
|---|---|
| LOW | Fewer than 8 |
| MID | 8 to 14 |
| HIGH | 15 or more |

Customers with zero transactions are included and assigned to the LOW segment.

---

## Project Structure

```
customer-transaction-pipeline/
├── src/
│   ├── pipeline.py       # Orchestration, logging, API ingestion, file outputs
│   ├── clean.py          # Validation, standardisation, rejection tracking
│   └── transform.py      # Join, transaction counts, segmentation, summarisation
├── outputs/
│   ├── customers_clean.parquet   # Cleaned and transformed customer records
│   ├── summary.csv               # City level summary
│   └── rejected.json             # Rejected records with rejection reasons
├── logs/
│   └── pipeline.log      # Full pipeline run log
├── config.json            # Pipeline configuration (not committed)
├── config.example.json    # Example config with placeholder values
├── requirements.txt       # Python dependencies
└── README.md
```

---

## Data Sources

Two public REST API endpoints are used as mock data sources:

| Endpoint | Description |
|---|---|
| `https://jsonplaceholder.typicode.com/users` | Customer records |
| `https://jsonplaceholder.typicode.com/posts` | Transaction records |

### Customer Fields Used

| Field | Description |
|---|---|
| id | Customer ID |
| name | Full name — standardised to title case |
| email | Email address — standardised to lowercase, validated for format |
| address.city | City extracted from nested address object |
| company.name | Employer name |

### Transaction Fields Used

| Field | Description |
|---|---|
| id | Transaction ID |
| userId | Customer ID — foreign key to customers |
| title | Transaction description |
| body | Transaction notes |

---

## Outputs

### customers_clean.parquet
Cleaned and transformed customer records with the following derived columns:

| Column | Description |
|---|---|
| city | Extracted from nested address object |
| row_count | Total number of transactions per customer |
| customer_segment | LOW, MID or HIGH based on transaction count |

### summary.csv
Aggregated city level summary:

| Column | Description |
|---|---|
| city | City name |
| total_customers_per_city | Unique customer count |
| avg_transactions_per_customer | Mean transaction count per customer |
| high_count | Number of HIGH segment customers |
| mid_count | Number of MID segment customers |
| low_count | Number of LOW segment customers |

### rejected.json
Rejected customer records with a `rejection_reason` field explaining why each record failed validation.

---

## Installation & Usage

**Prerequisites:** Python 3.8+, pip

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Create config file:**
```bash
cp config.example.json config.json
```

Your `config.json` should look like this:
```json
{
    "users_url": "https://jsonplaceholder.typicode.com/users",
    "transactions_url": "https://jsonplaceholder.typicode.com/posts",
    "log_filepath": "logs/pipeline.log",
    "output_dir": "outputs"
}
```

**Run the pipeline:**
```bash
python src/pipeline.py config.json
```

**Expected output:**
```
outputs/customers_clean.parquet
outputs/summary.csv
outputs/rejected.json
logs/pipeline.log
```

---

## Error Handling

The pipeline handles the following failure scenarios gracefully:

- Config file not found or malformed — logs error and exits
- API connection error, timeout or HTTP error — logs error and exits
- Output write failures — logs error per file, returns overall success status

---

## Technologies

| Technology | Usage |
|---|---|
| Python 3 | Core language |
| pandas | Data processing and transformation |
| NumPy | Vectorised conditional logic |
| requests | HTTP API calls |
| pyarrow | Parquet file output |
| logging | Pipeline run logging |
| Git | Version control |

---

## Design Decisions

**Separation of concerns** — pipeline logic is split across three files. `pipeline.py` handles orchestration, ingestion and file outputs. `clean.py` handles validation. `transform.py` handles joining, segmentation and summarisation.

**Config driven** — all URLs and file paths are externalised to `config.json` so the pipeline can be run in different environments without code changes.

**First failure rejection tracking** — records are only assigned their first rejection reason, avoiding misleading stacked error messages.

**Customers with zero transactions** — included in the output and assigned to the LOW segment, giving the analytics team full visibility of the customer base.

**Generic API function** — a single `pull_api()` function handles all endpoint calls, keeping the ingestion logic DRY and consistent.
