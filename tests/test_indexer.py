import shutil
from pathlib import Path
import pandas as pd
import pydicom.data
from src.dicom_indexer.indexer import load_dicom, extract_metadata, index_dicom_folder, save_to_parquet

def test_load_valid_dicom():
    """Test that a valid DICOM file loads successfully."""
    dcm_path = pydicom.data.get_testdata_file("CT_small.dcm")
    ds = load_dicom(Path(dcm_path))
    assert ds is not None
    assert hasattr(ds, "PatientID")

def test_load_invalid_file(tmp_path):
    """Test that loading a non-DICOM file returns None."""
    bad_file = tmp_path / "not_a_dicom.dcm"
    bad_file.write_text("this is not a dicom file")

    ds = load_dicom(bad_file)
    assert ds is None

def test_extract_metadata_fields():
    """Test that metadata extraction returns expected fields."""
    dcm_path = pydicom.data.get_testdata_file("MR_small.dcm")
    ds = load_dicom(Path(dcm_path))
    metadata = extract_metadata(ds, Path(dcm_path))

    assert metadata is not None
    assert "PatientID" in metadata
    assert "Modality" in metadata
    assert "StudyDate" in metadata

def test_index_dicom_folder(tmp_path):
    """Test indexing a folder of DICOM files."""
    test_folder = tmp_path / "sample_data"
    test_folder.mkdir()
    shutil.copy(pydicom.data.get_testdata_file("CT_small.dcm"), test_folder / "sample1.dcm")

    df = index_dicom_folder(test_folder)
    assert not df.empty
    assert "Modality" in df.columns
    assert "PatientID" in df.columns

def test_save_and_load_parquet(tmp_path):
    """Test saving a DataFrame to Parquet and reading it back."""
    df = pd.DataFrame([
        {"PatientID": "1", "Modality": "CT", "StudyDate": "20240101"},
        {"PatientID": "2", "Modality": "MR", "StudyDate": "20240102"}
    ])

    output_dir = tmp_path / "output_parquet"
    save_to_parquet(df, output_dir, partition_cols=["Modality"])

    reloaded = pd.read_parquet(output_dir)
    assert not reloaded.empty
    assert set(reloaded.columns) >= {"PatientID", "Modality", "StudyDate"}
