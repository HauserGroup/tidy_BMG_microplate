# Standard library imports
from datetime import datetime
from pathlib import Path
from typing import Any
import re
import sys

# Third-party imports
import numpy as np
import pandas as pd
import typer

# Constants
DEFAULT_WELL_ROW_IDX = 9


def load_dataframe(file_path: str, sheet_name: int | str = 0) -> pd.DataFrame:
    """
    Load DataFrame from CSV or Excel file.

    Args:
        file_path: Path to the file
        sheet_name: Sheet index or name for Excel files (default: 0)

    Returns:
        Loaded DataFrame
    """
    path = Path(file_path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(file_path, header=None)
    else:
        return pd.read_excel(file_path, header=None, sheet_name=sheet_name)


def generate_output_filename(raw_file_path: str) -> str:
    """
    Generate output filename based on the input file.

    Args:
        raw_file_path: Path to the raw input file

    Returns:
        Full path to the generated output file
    """
    # Extract base name from raw file
    raw_path = Path(raw_file_path)
    raw_basename = raw_path.stem

    # Create sensible output name with milliseconds to avoid collisions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:17]  # Include milliseconds
    output_basename = f"{raw_basename}_processed_{timestamp}.xlsx"

    # Determine output directory - use current working directory
    output_dir = Path.cwd() / "output"

    # Create output directory if it doesn't exist
    output_dir.mkdir(exist_ok=True)

    output_file_path = output_dir / output_basename
    print(f"[INFO] Auto-generated output filename: {output_file_path}")

    return str(output_file_path)


def parse_well_columns(
    df_raw: pd.DataFrame, well_row_idx: int = DEFAULT_WELL_ROW_IDX
) -> dict[int, str]:
    """
    Parse raw data to map column indices to well names.

    Args:
        df_raw: Raw data DataFrame
        well_row_idx: Row index containing well names (default: 9)

    Returns:
        Dictionary mapping column index to well name
    """
    well_names = df_raw.iloc[well_row_idx, :].values
    col_to_well = {}
    for c, val in enumerate(well_names):
        if isinstance(val, str) and re.match(r"^[A-H]\d{2}$", val):
            col_to_well[c] = val

    print("\n--- Raw Data Analysis ---")
    print(f"[INFO] Found {len(col_to_well)} wells in raw data (row {well_row_idx}):")
    wells_list = sorted(col_to_well.values())
    print(f"  Range: {wells_list[0]} to {wells_list[-1]}")
    print(
        f"  Wells: {', '.join(wells_list[:12])}{'...' if len(wells_list) > 12 else ''}"
    )

    return col_to_well


def extract_ratio_data(
    df_raw: pd.DataFrame, col_to_well: dict[int, str]
) -> tuple[dict[tuple[int | float, str], Any], list[int | float]]:
    """
    Extract ratio data from the raw DataFrame.

    Args:
        df_raw: Raw data DataFrame
        col_to_well: Mapping of column indices to well names

    Returns:
        Tuple of (ratio_data dictionary, sorted list of time points)
    """
    ratio_start_rows = df_raw[df_raw[0].str.contains("Ratio based on", na=False)].index
    if len(ratio_start_rows) == 0:
        raise ValueError(
            "Could not find 'Ratio based on' data block in the raw file. "
            "Please verify the input file format matches BMG output format."
        )

    start_row = ratio_start_rows[0]
    ratio_data = {}  # (Time, Well_Normalized) -> Value
    all_times = set()

    for idx in range(start_row, len(df_raw)):
        row = df_raw.iloc[idx]
        try:
            t_val = float(row[1])
            if t_val.is_integer():
                t_val = int(t_val)
            all_times.add(t_val)
        except (ValueError, TypeError):
            continue

        for c, w in col_to_well.items():
            val = row[c]
            ratio_data[(t_val, w)] = val

    sorted_times = sorted(list(all_times))
    print(
        f"Extracted data for {len(sorted_times)} time points ({min(sorted_times)}s to {max(sorted_times)}s)."
    )

    return ratio_data, sorted_times


def parse_template_structure(
    df_template: pd.DataFrame,
) -> tuple[pd.DataFrame, list[tuple[int, int, str]], list[int]]:
    """
    Parse template to identify well positions and header rows.

    Args:
        df_template: Template DataFrame

    Returns:
        Tuple of (master_strip DataFrame, well_cells list, header_rows list)
    """
    master_strip = df_template.iloc[:, 1:3].copy()
    well_cells = []
    header_rows = []

    print("\n--- Template Structure Analysis ---")
    for r in range(len(master_strip)):
        for c in range(2):
            val = master_strip.iloc[r, c]
            if isinstance(val, str):
                if "Time [s]" in val:
                    if r not in header_rows:
                        header_rows.append(r)
                        print(
                            f"[INFO] Found header row at template row {r}, col {c}: '{val}'"
                        )
                elif re.match(r"^[A-H]\d+$", val):
                    match = re.match(r"^([A-H])(\d+)$", val)
                    w_norm = f"{match.group(1)}{int(match.group(2)):02d}"
                    well_cells.append((r, c, w_norm))

    # Display well mapping
    print(f"\n[INFO] Found {len(well_cells)} well positions in template:")
    well_by_row = {}
    for r, c, w in well_cells:
        if r not in well_by_row:
            well_by_row[r] = []
        well_by_row[r].append(f"{w}(col{c})")

    for row_idx in sorted(well_by_row.keys())[:10]:  # Show first 10 rows
        wells_str = ", ".join(well_by_row[row_idx])
        print(f"  Row {row_idx}: {wells_str}")
    if len(well_by_row) > 10:
        print(f"  ... and {len(well_by_row) - 10} more rows")

    return master_strip, well_cells, header_rows


def verify_well_coverage(
    well_cells: list[tuple[int, int, str]], col_to_well: dict[int, str]
) -> None:
    """
    Verify and report well coverage between template and raw data.

    Args:
        well_cells: List of well cell positions from template
        col_to_well: Mapping of column indices to well names from raw data
    """
    wells_in_template = set([w for _, _, w in well_cells])
    wells_in_raw = set(col_to_well.values())

    missing_in_template = wells_in_raw - wells_in_template
    extra_in_template = wells_in_template - wells_in_raw

    if missing_in_template:
        print(
            f"\n[WARNING] Wells in raw data but NOT in template: {sorted(missing_in_template)}"
        )
    if extra_in_template:
        print(
            f"[WARNING] Wells in template but NOT in raw data: {sorted(extra_in_template)}"
        )

    matching_wells = wells_in_template & wells_in_raw
    print(
        f"\n[INFO] Matching wells: {len(matching_wells)}/{len(wells_in_raw)} from raw data"
    )
    print(
        f"[INFO] Template coverage: {len(matching_wells)}/{len(wells_in_template)} wells will have data"
    )


def generate_output_table(
    df_template: pd.DataFrame,
    master_strip: pd.DataFrame,
    sorted_times: list[int | float],
    header_rows: list[int],
    well_cells: list[tuple[int, int, str]],
    ratio_data: dict[tuple[int | float, str], Any],
) -> pd.DataFrame:
    """
    Generate the final output table by combining template with ratio data for all time points.

    Args:
        df_template: Template DataFrame
        master_strip: Master strip from template (columns 1-2)
        sorted_times: List of time points
        header_rows: List of header row indices
        well_cells: List of well cell positions
        ratio_data: Dictionary of ratio values

    Returns:
        Final combined DataFrame
    """
    result_parts = []

    # Add index column
    result_parts.append(df_template.iloc[:, 0:1])

    for t in sorted_times:
        current_strip = master_strip.copy()

        for r in header_rows:
            current_strip.iloc[r, 0] = f"{t} (Time [s])"
            current_strip.iloc[r, 1] = np.nan

        for r, c, w_norm in well_cells:
            val = ratio_data.get((t, w_norm))
            current_strip.iloc[r, c] = val if val is not None else np.nan

        result_parts.append(current_strip)

    return pd.concat(result_parts, axis=1)


def display_sample_verification(
    df_final: pd.DataFrame,
    well_cells: list[tuple[int, int, str]],
    sorted_times: list[int | float],
    ratio_data: dict[tuple[int | float, str], Any],
) -> None:
    """
    Display sample output mapping for verification.

    Args:
        df_final: Final output DataFrame
        well_cells: List of well cell positions
        sorted_times: List of time points
        ratio_data: Dictionary of ratio values
    """
    print("\n--- Sample Output Verification ---")
    print("[INFO] Sample of first time point mapping (first 5 wells):")
    first_time = sorted_times[0]
    for r, c, w in well_cells[:5]:
        col_idx = 1 + c  # First time strip starts at col 1
        output_val = df_final.iloc[r, col_idx]
        input_val = ratio_data.get((first_time, w))
        match_status = (
            "✓"
            if (pd.isna(output_val) and pd.isna(input_val)) or output_val == input_val
            else "✗"
        )
        print(
            f"  {match_status} Row {r}, Col {col_idx}: Well {w} -> Raw: {input_val}, Output: {output_val}"
        )


def verify_data_integrity(
    df_final: pd.DataFrame,
    ratio_data: dict[tuple[int | float, str], Any],
    sorted_times: list[int | float],
    well_cells: list[tuple[int, int, str]],
    header_rows: list[int],
) -> None:
    """
    Performs stringent checks to ensure output data matches input data exactly.
    """
    print("\n--- Starting Comprehensive Data Verification ---")

    errors = []
    checks_passed = 0
    total_checks = 0

    # 1. Verify Structure Dimensions
    # Expected columns: 1 (Index col) + (Number of Time Points * 2 columns per strip)
    expected_cols = 1 + (len(sorted_times) * 2)
    if df_final.shape[1] != expected_cols:
        errors.append(
            f"Dimension Error: Expected {expected_cols} columns, found {df_final.shape[1]}"
        )
    else:
        print(f"[PASS] Output dimensions correct: {df_final.shape}")

    # 2. Verify Time Headers
    print("[INFO] Verifying Time Headers...")
    for t_idx, t_val in enumerate(sorted_times):
        # Calculate where this time strip starts.
        # Col 0 is the labels. First time strip starts at col 1.
        col_idx = 1 + (t_idx * 2)

        # Check the header row(s) for the correct time label
        # We check the first registered header row
        header_r = header_rows[0]
        cell_val = df_final.iloc[header_r, col_idx]

        expected_header = f"{t_val} (Time [s])"
        if str(cell_val) != expected_header:
            errors.append(
                f"Header Mismatch at Index {t_idx}: Expected '{expected_header}', found '{cell_val}'"
            )

    # 3. Verify Every Single Data Point
    print(
        f"[INFO] Verifying {len(sorted_times) * len(well_cells)} individual data points..."
    )

    for t_idx, t_val in enumerate(sorted_times):
        col_offset = 1 + (t_idx * 2)

        for r_template, c_template, w_norm in well_cells:
            total_checks += 1

            # The row index in df_final is the same as the template (r_template)
            # The col index is the offset + the relative column in the strip (c_template)
            actual_col = col_offset + c_template

            output_val = df_final.iloc[r_template, actual_col]
            input_val = ratio_data.get((t_val, w_norm))

            # Check logic
            is_match = False

            # Case A: Both are NaN/None
            if (pd.isna(output_val) or output_val == "") and (
                pd.isna(input_val) or input_val is None
            ):
                is_match = True

            # Case B: Both are numeric and close (floating point tolerance)
            elif isinstance(output_val, (int, float)) and isinstance(
                input_val, (int, float)
            ):
                if np.isclose(output_val, input_val, equal_nan=True):
                    is_match = True

            # Case C: Exact string match (unlikely for data, but safe to keep)
            elif output_val == input_val:
                is_match = True

            if not is_match:
                errors.append(
                    f"Data Mismatch -> Time: {t_val}s, Well: {w_norm}. "
                    f"Raw: {input_val}, Output: {output_val}"
                )
            else:
                checks_passed += 1

    # --- Report ---
    if errors:
        print(f"\n[FAIL] Verification Failed with {len(errors)} errors.")
        for e in errors[:10]:  # Print first 10 errors
            print(f" - {e}")
        if len(errors) > 10:
            print(f" ... and {len(errors) - 10} more.")
        raise ValueError(
            "Data verification failed. Output file was NOT generated to prevent corruption."
        )
    else:
        print(f"[PASS] All {checks_passed} data points verified successfully.")
        print("--- Verification Complete ---\n")


def process_plate_data(
    raw_file_path: str,
    template_file_path: str,
    output_file_path: str | None = None,
    raw_sheet: int | str = 0,
    template_sheet: int | str = 0,
) -> None:
    """
    Reads a BMG Raw Data file, extracts 'Ratio' values, maps them
    into a layout defined by a template file, and verifies integrity.

    Args:
        raw_file_path: Path to the BMG raw data file
        template_file_path: Path to the template file
        output_file_path: Path for the output file (optional). If None, generates
                         a sensible name based on the input file.
        raw_sheet: Sheet index (0-based) or sheet name for raw data file (default: 0)
        template_sheet: Sheet index (0-based) or sheet name for template file (default: 0)
    """

    # Generate output filename if not provided
    if output_file_path is None:
        output_file_path = generate_output_filename(raw_file_path)

    print(f"Processing Raw File: {raw_file_path}")
    print(f"Using Template: {template_file_path}")

    # --- 1. Validate and Load Data ---
    # Check file existence
    raw_path = Path(raw_file_path)
    template_path = Path(template_file_path)

    if not raw_path.exists():
        print(f"Error: Raw file not found: {raw_file_path}", file=sys.stderr)
        sys.exit(1)

    if not template_path.exists():
        print(f"Error: Template file not found: {template_file_path}", file=sys.stderr)
        sys.exit(1)

    # Load files
    try:
        df_raw = load_dataframe(raw_file_path, raw_sheet)
        df_template = load_dataframe(template_file_path, template_sheet)
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error loading files: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 2. Parse Raw Data to Map Columns to Wells ---
    col_to_well = parse_well_columns(df_raw)

    # --- 3. Extract Ratio Data ---
    try:
        ratio_data, sorted_times = extract_ratio_data(df_raw, col_to_well)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 4. Parse Template Structure ---
    master_strip, well_cells, header_rows = parse_template_structure(df_template)

    # --- 5. Verify Well Coverage ---
    verify_well_coverage(well_cells, col_to_well)

    # --- 6. Generate Full Table ---
    df_final = generate_output_table(
        df_template, master_strip, sorted_times, header_rows, well_cells, ratio_data
    )

    # --- 7. VERIFICATION STEP ---
    verify_data_integrity(df_final, ratio_data, sorted_times, well_cells, header_rows)

    # --- 8. Display Sample Verification ---
    display_sample_verification(df_final, well_cells, sorted_times, ratio_data)

    # --- 9. Save Output ---
    df_final.to_excel(output_file_path, index=False, header=False)
    print(
        f"\n✓ Successfully processed {len(sorted_times)} time points across {len(well_cells)} wells"
    )
    print(f"✓ Output saved to: {output_file_path}")


def main(
    raw_file: str = typer.Argument(
        ..., help="Path to the BMG raw data file (Excel or CSV)"
    ),
    template_file: str = typer.Argument(
        ..., help="Path to the template file (Excel or CSV)"
    ),
    output_file: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path (auto-generated if not specified)",
    ),
    raw_sheet: str = typer.Option(
        "0", "--raw-sheet", help="Sheet index or name for raw data file (default: 0)"
    ),
    template_sheet: str = typer.Option(
        "0",
        "--template-sheet",
        help="Sheet index or name for template file (default: 0)",
    ),
) -> None:
    """
    Process BMG plate data by mapping raw ratio values to a template layout.

    This tool reads BMG Raw Data files, extracts 'Ratio' values, and maps them
    into a layout defined by a template file for all time points.

    Example usage:

        python script.py data/plate1.xlsx templates/template.xlsx

        python script.py data/plate1.xlsx templates/template.xlsx -o output/result.xlsx

        python script.py data/plate1.xlsx templates/template.xlsx --raw-sheet 1

        python script.py data/plate1.xlsx templates/template.xlsx --template-sheet "Sheet3"
    """
    # Convert sheet parameters to int if they're numeric, otherwise keep as string
    raw_sheet_param: int | str = int(raw_sheet) if raw_sheet.isdigit() else raw_sheet
    template_sheet_param: int | str = (
        int(template_sheet) if template_sheet.isdigit() else template_sheet
    )

    process_plate_data(
        raw_file, template_file, output_file, raw_sheet_param, template_sheet_param
    )


if __name__ == "__main__":
    typer.run(main)
