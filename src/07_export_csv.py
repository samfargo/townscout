"""
Export final data to CSV for Pro users.
"""
import argparse
import os
import pandas as pd


def main():
    """Main function to export data to CSV."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input parquet file with hex data to export.")
    ap.add_argument("--output", required=True, help="Output CSV file path.")
    args = ap.parse_args()

    print(f"--- Exporting data from {args.input} to {args.output} ---")
    
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}")

    # TODO: Implement the final selection and formatting of columns for export.
    print("[stub] CSV export not yet fully implemented.")
    
    df = pd.read_parquet(args.input)
    
    # For now, just a direct export. This can be refined.
    df.to_csv(args.output, index=False)
    
    print(f"[ok] Data exported to {args.output}")


if __name__ == "__main__":
    main()
