#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

HUMAN_INPUT = "all_human_cell_lines_cleaned.csv"
ANIMAL_INPUT = "all_animal_cell_lines_cleaned.csv"
OUTPUT_FILE = "combined_atcc_cell_lines.csv"


def main():
    print("=== Loading cleaned input files ===")
    human_df = pd.read_csv(HUMAN_INPUT)
    animal_df = pd.read_csv(ANIMAL_INPUT)

    # Tag where each row came from (optional but useful)
    human_df["source_group"] = "human"
    animal_df["source_group"] = "animal"

    # Make sure they share the same set of columns
    all_cols = sorted(set(human_df.columns).union(animal_df.columns))
    human_df = human_df.reindex(columns=all_cols)
    animal_df = animal_df.reindex(columns=all_cols)

    print(f"HUMAN rows:  {len(human_df)}")
    print(f"ANIMAL rows: {len(animal_df)}")

    # Concatenate
    combined = pd.concat([human_df, animal_df], ignore_index=True)

    # Add CLO_ID column if it doesn't exist yet
    if "CLO_ID" not in combined.columns:
        combined["CLO_ID"] = ""

    # Optional: sort by atcc_id if that column exists
    if "atcc_id" in combined.columns:
        combined = combined.sort_values("atcc_id").reset_index(drop=True)

    print(f"Combined total rows: {len(combined)}")

    # Save
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"=== Done. Saved combined file to: {OUTPUT_FILE} ===")


if __name__ == "__main__":
    main()
