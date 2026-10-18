# Employee & Employer Compensation — Azure Data Engineering

A portfolio-ready starter project demonstrating an Azure-style batch ETL pipeline for employee and employer compensation data using PySpark, Delta Lake, and the Bronze/Silver/Gold Medallion Architecture.

> **Honest project status:** This repository contains runnable local PySpark examples and Azure deployment templates. Azure Data Factory, ADLS Gen2, and Azure Databricks resources are not provisioned automatically. Configure your own Azure subscription and credentials before deploying.

## Architecture

```text
SQL Server / Oracle / CSV
          |
          v
 Azure Data Factory (orchestration)
          |
          v
 ADLS Gen2 - Bronze (raw source files)
          |
          v
 Azure Databricks + PySpark
   cleansing, validation, business rules
          |
          v
 ADLS Gen2 - Silver (validated Delta)
          |
          v
 Delta MERGE / incremental upsert
          |
          v
 ADLS Gen2 - Gold (reporting aggregates)
```

## Features

- PySpark ingestion from CSV/flat files
- Data cleansing, schema enforcement, and validation
- Bronze, Silver, and Gold layer examples
- Delta Lake table writes and incremental `MERGE` example
- Compensation business rules and exception output
- Reporting aggregate for employer-level compensation
- Basic logging and a unit-testable transformation design
- Azure Data Factory pipeline template for customization

## Repository structure

```text
employee-employer-compensation/
├── config/
│   └── project_config.json
├── data/
│   └── sample/
│       └── employee_compensation.csv
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_reporting.py
│   └── 04_delta_incremental_merge.py
├── pipelines/
│   └── adf/
│       └── pl_employee_compensation.json
├── src/
│   └── compensation_pipeline.py
├── tests/
│   └── test_business_rules.py
├── .gitignore
├── requirements.txt
└── README.md
```

## Sample data

The included CSV is synthetic sample data created for learning. It does not contain real employee records.

Columns:
- `employee_id`
- `employee_name`
- `employer_id`
- `employer_name`
- `department`
- `annual_salary`
- `employer_contribution`
- `currency`
- `effective_date`
- `updated_at`

## Run locally

Recommended: Python 3.10 or 3.11 with Java installed for PySpark.

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

pip install -r requirements.txt
python src/compensation_pipeline.py
```

The local script writes CSV outputs under `output/` and demonstrates the transformations without requiring Azure credentials.

## Run with Azure Databricks / Delta Lake

1. Create an Azure Data Lake Storage Gen2 account and a container.
2. Configure secure access using a managed identity or service principal stored in a secret scope. Do not hard-code secrets.
3. Import the notebooks from `notebooks/` into Azure Databricks.
4. Install compatible `pyspark` and `delta-spark` versions for your Spark runtime, or use the Delta Lake runtime already provided by Databricks.
5. Update the storage paths and catalog/database names in the notebook configuration cells.
6. Configure ADF linked services, datasets, parameters, and triggers. The JSON under `pipelines/adf/` is a starting template, not a ready-to-deploy pipeline.
7. Run a test batch, inspect rejected records, and verify the Gold output before scheduling production runs.

## Data quality rules demonstrated

- Required employee and employer identifiers must be present.
- Salary and employer contribution must be non-negative numeric values.
- Effective date must be parseable.
- Duplicate employee/effective-date records are reduced to the most recently updated record.
- Invalid rows are written to a rejected-record output for review.

Adapt these rules to the actual business definitions before production use.

## Security

- Never commit credentials, access keys, connection strings, or production data.
- Use managed identity / secret scopes and least-privilege permissions.
- Replace the synthetic sample dataset before adapting this to a real business process.
- Review privacy, retention, and access-control requirements for compensation data.

## Resume bullet suggestions

Use only bullets that match what you have actually implemented and tested:

- Developed PySpark ETL transformations to cleanse and validate employee and employer compensation data.
- Designed a Bronze/Silver/Gold Medallion data flow and Delta Lake incremental upsert pattern.
- Created a Gold reporting dataset summarizing compensation by employer and department.
- Prepared an Azure Data Factory orchestration template and documented secure ADLS Gen2 integration.

## Next improvements

- Add SQL Server and Oracle linked-service configurations in your own Azure environment.
- Add ADF pipeline parameters, retry policy, and failure notifications.
- Add data-quality metrics, audit tables, and CI/CD deployment.
- Add screenshots of successful runs only after testing in Azure.
