import pandas as pd
import re
import zipfile
from datetime import datetime

# === VALIDATION OF XLSX ===
file_path = r"C:/Users/sjt/Documents/Product list 1001 food study.xlsx"
print("Is valid XLSX (ZIP-based)?", zipfile.is_zipfile(file_path))

# === CONFIGURATION ===
csv_file_liq = r"C:/Users/sjt/Downloads/2025-08-28T07-26_export.csv"   # == FILES FROM NN002120
csv_file_sol = r"C:/Users/sjt/Downloads/2025-08-27T06-55_export.csv"   # == FILES FROM NN002188 AND NN002217
excel_file   = r"C:/Users/sjt/Documents/Product list 1001 food study.xlsx"
sheet_name_liq = 'Products List-Liquids'
sheet_name_sol = 'Products List-solids'

# === COMMON DROP & METRIC COLUMNS ===
drop_cols = ["Average", "std", "RSD", "extra1", "extra2", "extra3", 
             "exrta1", "extra2", "extra3"]
metric_cols = ["Mean", "Std", "RSD%"]

# === HELPER FUNCTIONS ===
def extract_numeric_suffix(val):
    match = re.findall(r'(\d{8})(?!.*\d)', str(val))
    return match[0] if match else None

def process_dataset(df, sheet_name):
    #"""Takes a dataframe and sheet name, returns the updated dataframe ready for save."""
    # Keep only desired jobnames
    
# === MAKE A LIST OF ALLOWED JOBNAMES, DISREGARD EVERYTHING ELSE    
    allowed_jobnames = ["Na7xnative", "Na5xnative", "Na 5x native ","Na3x", "Na4xnative","Na 5x","Na5xnative_01","Na5x-noscale","Liquids-Na"]  # change to your list
    df = df[df['jobname'].isin(allowed_jobnames)].copy()
    df['time'] = pd.to_datetime(df['time'])
    # Earliest time per tag
    start_times = df.groupby('tag')['time'].min().reset_index()
    start_times.rename(columns={'time': 'start_time'}, inplace=True)
    df = df.merge(start_times, on='tag')

    # Extract last 8-digit suffix
    df['job_suffix'] = df['tag'].apply(extract_numeric_suffix)
    df = df.dropna(subset=['job_suffix'])
    df = df[df['uiunit'].isin(['%wv', '%ww'])].copy()
    df = df[df['chemname']== 'Sodium'].copy()
    
    # Sort and label runs
    df.sort_values(by=['job_suffix', 'start_time'], inplace=True)
    df['Run'] = df.groupby('job_suffix').cumcount().apply(lambda x: f'run{x+1}')

    df = df[['jobname', 'job_suffix', 'tag', 'start_time', 'Run', 'concentration_gui']]

    # Load existing sheet
    existing_df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=4)
    # Normalize headers to strings, strip spaces
    existing_df.columns = existing_df.columns.map(str).str.strip()

    # Locate the "Bar code" column case-insensitively
    barcode_cols = [c for c in existing_df.columns if c.strip().lower().replace(" ", "") == "barcode"]
    if barcode_cols:
        existing_df.rename(columns={barcode_cols[0]: "job_suffix"}, inplace=True)
    else:
        raise ValueError(f"No 'Bar code' column found in sheet '{sheet_name}'. "
                         f"Available columns: {list(existing_df.columns)}")

    existing_df = existing_df.drop(columns=drop_cols, errors='ignore')
    run_cols_exist = [c for c in existing_df.columns if isinstance(c, str) and c.lower().startswith('run')]
    existing_df = existing_df.drop(columns=run_cols_exist, errors='ignore')

    # Merge prep
    existing_df['suffix_key'] = existing_df['job_suffix'].astype(str).str[-8:]
    pivot_df = df.pivot(index='job_suffix', columns='Run', values='concentration_gui').reset_index()
    pivot_df = pivot_df.rename(columns={'job_suffix': 'suffix_key'})

    updated_df = pd.merge(existing_df, pivot_df, on='suffix_key', how='left')

    # Map Swissknife barcode
    tag_lookup = (df.drop_duplicates('job_suffix')
                    .set_index('job_suffix')['tag']
                    .to_dict())
    updated_df['Swissknife barcode'] = updated_df['suffix_key'].map(tag_lookup)

    updated_df = updated_df.drop(columns=['suffix_key'])

    # Sort run columns
    run_cols = [col for col in updated_df.columns if isinstance(col, str) and col.lower().startswith('run')]
    sorted_run_cols = sorted(run_cols, key=lambda x: int(re.search(r'\d+', x).group()))
    first_seven_runs = sorted_run_cols[:7]  # Only first 7

    non_run_cols = [col for col in updated_df.columns if col not in run_cols]
    updated_df = updated_df[non_run_cols + sorted_run_cols]

    # Rename back
    if 'job_suffix' in updated_df.columns:
        updated_df.rename(columns={'job_suffix': 'Bar code'}, inplace=True)

    # Calculate metrics ONLY for first 7 runs
    stats_df = updated_df[["Bar code"] + first_seven_runs].copy()
    stats_df["Mean"] = stats_df[first_seven_runs].mean(axis=1)
    stats_df["Std"] = stats_df[first_seven_runs].std(axis=1)
    stats_df["RSD%"] = 100 * (stats_df["Std"] / stats_df["Mean"])

    updated_df["Mean"] = stats_df["Mean"]
    updated_df["Std"] = stats_df["Std"]
    updated_df["RSD%"] = stats_df["RSD%"]

    # Place metrics before runs
    col_order = [c for c in updated_df.columns if c not in metric_cols]
    if first_seven_runs:
        first_run_idx = min(col_order.index(c) for c in first_seven_runs)
        for offset, c in enumerate(metric_cols):
            col_order.insert(first_run_idx + offset, c)
    updated_df = updated_df[col_order]

    return updated_df, len(df)

# === LOAD CSV FILES ===
df_liq = pd.read_csv(csv_file_liq)
df_sol = pd.read_csv(csv_file_sol)

# === PROCESS BOTH ===
updated_df_liq, count_liq = process_dataset(df_liq, sheet_name_liq)
updated_df_sol, count_sol = process_dataset(df_sol, sheet_name_sol)

# === SAVE OUTPUT WITH BOTH SHEETS ===
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
xls_path = f'C:/Users/sjt/Documents/Product list 1001 food study_{timestamp}.xlsx'

with pd.ExcelWriter(xls_path, engine='xlsxwriter') as writer:
    updated_df_liq.to_excel(writer, sheet_name=sheet_name_liq, index=False)
    updated_df_sol.to_excel(writer, sheet_name=sheet_name_sol, index=False)

print(f"✅ Saved summary to: {xls_path}")
print(f"✅ Appended {count_liq} liquid instances and {count_sol} solid instances with valid job suffixes")