import pydicom
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging

def load_dicom(file_path: Path) -> Optional[pydicom.dataset.Dataset]:
    """Load DICOM without heavy pixel data."""
    try:
        ds = pydicom.dcmread(file_path, stop_before_pixels=True)  # Don't load heavy pixel data
        return ds
    
    except Exception as e:
        logging.warning(f"Failed to read {file_path}: {e}")
        return None

def extract_metadata(ds, file_path: Path) -> Optional[Dict]:
    """Extract relevant fields from a DICOM dataset."""
    if ds is None:
        return None

    metadata = {
        # Core identifiers
        "PatientID": getattr(ds, "PatientID", None),
        "StudyInstanceUID": getattr(ds, "StudyInstanceUID", None),
        "SeriesInstanceUID": getattr(ds, "SeriesInstanceUID", None),
        "SOPInstanceUID": getattr(ds, "SOPInstanceUID", None), # Uniquely identifies single instance
        "Modality": getattr(ds, "Modality", None),
        "StudyDate": getattr(ds, "StudyDate", None),
        "SeriesDescription": getattr(ds, "SeriesDescription", None),
        "SOPClassUID": getattr(ds, "SOPClassUID", None), 

        # Acquisition details
        "Manufacturer": getattr(ds, "Manufacturer", None),
        "ManufacturerModelName": getattr(ds, "ManufacturerModelName", None),
        "SliceThickness": getattr(ds, "SliceThickness", None),
        "KVP": getattr(ds, "KVP", None), # CT/X-ray

        # Patient-level
        "PatientSex": getattr(ds, "PatientSex", None),
        "PatientBirthDate": getattr(ds, "PatientBirthDate", None),

        # File info
        "FilePath": str(file_path),
        "FileSizeBytes": file_path.stat().st_size,
    }
    return metadata

def index_dicom_folder(input_dir: Union[str, Path]) -> pd.DataFrame:
    """Walk through the folder and build a DICOM metadata index."""
    input_dir = Path(input_dir)
    records = []

    for i, dcm_file in enumerate(input_dir.glob("**/*.dcm")):
        if not dcm_file.is_file():
            continue
        try:
            ds = load_dicom(dcm_file)
            metadata = extract_metadata(ds, dcm_file)
            if metadata:
                records.append(metadata)
        except Exception as e:
            logging.warning(f"Failed to process {dcm_file}: {e}")
        if i % 100 == 0:
            logging.info(f"Processed {i+1} files...")

    return pd.DataFrame(records)

def save_to_parquet(
    df: pd.DataFrame,
    output_dir: Path,
    partition_cols: Optional[List[str]] = None
) -> None:
    """Save a dataframe to partitioned Parquet files, handling missing partition values.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if partition_cols:
        # Drop rows that have missing partition values
        missing_before = len(df)
        df = df.dropna(subset=partition_cols)
        missing_after = len(df)
        if missing_before != missing_after:
            logging.warning(f"Dropped {missing_before - missing_after} rows due to missing partition column values.")

    df.to_parquet(
        output_dir,
        index=False,
        partition_cols=partition_cols,
        engine="pyarrow",
    )
    logging.info(f"Saved partitioned index to {output_dir}")

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Index DICOM files into Parquet.")
    parser.add_argument("--input-dir", type=str, required=True, help="Input directory containing DICOM files")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory for Parquet files")
    parser.add_argument("--partition-cols", type=str, nargs="*", default=["Modality", "StudyDate"],
                        help="Columns to partition by (default: Modality and StudyDate)")
    args = parser.parse_args()

    df = index_dicom_folder(args.input_dir)
    save_to_parquet(df, args.output_dir, partition_cols=args.partition_cols)
