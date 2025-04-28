import argparse
import logging
from pathlib import Path
import duckdb
from indexer import index_dicom_folder, save_to_parquet

def index_command(args):
    """Handle the 'index' subcommand."""
    print("Running index_command()...")
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    logging.info(f"Indexing DICOM files from {input_dir}...")
    df = index_dicom_folder(input_dir)

    logging.info(f"Saving indexed metadata to {output_dir}...")
    save_to_parquet(df, output_dir, partition_cols=args.partition_cols)

    logging.info(f"Done! Indexed {len(df)} files.")

def query_command(args):
    """Handle the 'query' subcommand."""
    parquet_path = Path(args.parquet_path)
    sql_query = args.query

    logging.info(f"Running query on {parquet_path}...")
    con = duckdb.connect()

    con.execute(f"""
        CREATE TABLE dicom_index AS
        SELECT * FROM parquet_scan('{parquet_path}/**/*.parquet');
    """)
    result = con.execute(sql_query).fetchdf()

    print(result)

def main():
    parser = argparse.ArgumentParser(description="DICOM Indexer CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Index subcommand
    index_parser = subparsers.add_parser("index", help="Index DICOM files into Parquet")
    index_parser.add_argument("--input-dir", type=str, required=True)
    index_parser.add_argument("--output-dir", type=str, required=True)
    index_parser.add_argument("--partition-cols", type=str, nargs="*", default=["Modality", "StudyDate"])
    index_parser.set_defaults(func=index_command)

    # Query subcommand
    query_parser = subparsers.add_parser("query", help="Query indexed DICOM metadata using SQL")
    query_parser.add_argument("--parquet-path", type=str, required=True)
    query_parser.add_argument("--query", type=str, required=True)
    query_parser.set_defaults(func=query_command)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    args.func(args)

if __name__ == "__main__":
    main()
