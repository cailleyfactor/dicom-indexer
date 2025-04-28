# DICOM Indexer

## Overview

This project implements a lightweight utility for indexing DICOM files into a partitioned Parquet dataset.  

The utility can:
- Traverse a folder of DICOM files
- Extract key metadata fields (e.g., PatientID, Modality, StudyDate)
- Save the metadata into partitioned Parquet files
- Allow flexible querying using SQL via DuckDB

A write up of design choices is included in the write_up directory.

---

## Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/dicom-indexer.git
    cd dicom-indexer
    ```

2. Create and activate a virtual environment (recommended):

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

---

## Usage

### Index DICOM Files

Index a directory of DICOM files into a partitioned Parquet dataset:

```bash
python src/dicom_indexer/cli.py index \
  --input-dir path/to/dicom_folder/ \
  --output-dir path/to/output_folder/ \
  --partition-cols Modality StudyDate
```

### Query Indexed Metadata

After indexing, you can query the metadata using SQL:

```bash
python src/dicom_indexer/cli.py query \
  --parquet-path path/to/output_folder/ \
  --query "SELECT Modality, COUNT(*) FROM dicom_index GROUP BY Modality;"
```

---
## Unit Tests

Basic unit tests are provided in `tests/test_indexer.py`.

To run the tests:

```bash
pip install pytest
PYTHONPATH=. pytest tests/

