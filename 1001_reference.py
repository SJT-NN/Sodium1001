import streamlit as st
import pandas as pd
from openpyxl import load_workbook
import re
from datetime import datetime
import io

# === HELPER FUNCTIONS ===
def extract_numeric_suffix(val):
    match = re.findall(r'(\d{8})(?!.*\d)', str(val))
    return match[0] if match else None

def process_dataset(df, sheet_name, excel_df):
    allowed_jobnames = [
        "Na7xnative", "Na5xnative", "Na 5x native ", "Na3x", "Na4xnative",
        "Na 5x", "Na5xnative_01", "Na5x-noscale", "Liquids-Na"
    ]
    drop_cols = ["Average", "std", "RSD", "extra1", "extra2", "extra3", "exrta1"]
    metric_cols = ["Mean", "Std", "RSD%"]

    df = df[df['jobname'].isin(allowed_jobnames)].copy()
    df['time'] = pd.to_datetime(df['time'])
    start_times = df.groupby('tag')['time'].min().reset_index()
    start_times.rename(columns={'time': 'start_time'}, inplace=True)
    df = df.merge(start_times, on='tag')

    df['job_suffix'] = df['tag'].apply(extract_numeric_suffix)
    df = df.dropna(subset=['job_suffix'])
    df = df[df['uiunit'].isin(['%wv', '%ww'])].copy()
    df = df[df['chemname'] == 'Sodium'].copy()

    df.sort_values(by=['job_suffix', 'start_time'], inplace=True)
    df['Run'] = df.groupby('job_suffix').cumcount().apply(lambda x: f'run{x+1}')
    df = df[['jobname', 'job_suffix', 'tag', 'start_time', 'Run', 'concentration_gui']]

    existing_df = excel_df.copy()
    existing_df.columns = existing_df.columns.map(str).str.strip()
    barcode_cols = [c for c in existing_df.columns if c.strip().lower().replace(" ", "") == "barcode"]
    if barcode_cols:
        existing_df.rename(columns={barcode_cols[0]: "job_suffix"}, inplace=True)
    else:
        st.error(f"No 'Bar code' column found in sheet '{sheet_name}'")
        return None, 0

    existing_df = existing_df.drop(columns=drop_cols, errors='ignore')
    run_cols_exist = [c for c in existing_df.columns if isinstance(c, str) and c.lower().startswith('run')]
    existing_df = existing_df.drop(columns=run_cols_exist, errors='ignore')

    existing_df['suffix_key'] = existing_df['job_suffix'].astype(str).str[-8:]
    pivot_df = df.pivot(index='job_suffix', columns='Run', values='concentration_gui').reset_index()
    pivot_df = pivot_df.rename(columns={'job_suffix': 'suffix_key'})
    updated_df = pd.merge(existing_df, pivot_df, on='suffix_key', how='left')

    tag_lookup = df.drop_duplicates('job_suffix').set_index('job_suffix')['tag'].to_dict()
    updated_df['Swissknife barcode'] = updated_df['suffix_key'].map(tag_lookup)
    updated_df = updated_df.drop(columns=['suffix_key'])

    run_cols = [col for col in updated_df.columns if isinstance(col, str) and col.lower().startswith('run')]
    sorted_run_cols = sorted(run_cols, key=lambda x: int(re.search(r'\d+', x).group()))
    first_seven_runs = sorted_run_cols[:7]
    non_run_cols = [col for col in updated_df.columns if col not in run_cols]
    updated_df = updated_df[non_run_cols + sorted_run_cols]

    if 'job_suffix' in updated_df.columns:
        updated_df.rename(columns={'job_suffix': 'Bar code'}, inplace=True)

    stats_df = updated_df[["Bar code"] + first_seven_runs].copy()
    stats_df["Mean"] = stats_df[first_seven_runs].mean(axis=1)
    stats_df["Std"] = stats_df[first_seven_runs].std(axis=1)
    stats_df["RSD%"] = 100 * (stats_df["Std"] / stats_df["Mean"])

    updated_df["Mean"] = stats_df["Mean"]
    updated_df["Std"] = stats_df["Std"]
    updated_df["RSD%"] = stats_df["RSD%"]

    # Define allowed columns
    allowed_prefix = "run"
    allowed_exact = ["Bar code", "Swissknife barcode", "Product Name", "Food group", "Producer", "Brand","Mean", "Std", "RSD%"]

    # Filter columns: keep if exact match or starts with "run" followed by digits
    filtered_columns = [
        col for col in updated_df.columns
        if col in allowed_exact or (isinstance(col, str) and re.match(rf"{allowed_prefix}\d+$", col))
    ]

    # Apply column filter
    updated_df = updated_df[filtered_columns]
    col_order = [c for c in updated_df.columns if c not in metric_cols]
    if first_seven_runs:
        first_run_idx = min(col_order.index(c) for c in first_seven_runs)
        for offset, c in enumerate(metric_cols):
            col_order.insert(first_run_idx + offset, c)
    updated_df = updated_df[col_order]

    return updated_df, len(df)

# === STREAMLIT UI ===
st.title("📊 1001 Food Study Barcode Data Correlator")

excel_file = st.file_uploader("Upload Excel file", type=["xlsx"])
csv_file_liq = st.file_uploader("Upload CSV file for Liquids", type=["csv"])
csv_file_sol = st.file_uploader("Upload CSV file for Solids", type=["csv"])

sheet_name_liq = 'Products List-Liquids'
sheet_name_sol = 'Products List-solids'

if excel_file and csv_file_liq and csv_file_sol:
    try:
        excel_data = pd.read_excel(excel_file, sheet_name=None, skiprows=4)
        df_liq = pd.read_csv(csv_file_liq)
        df_sol = pd.read_csv(csv_file_sol)

        updated_df_liq, count_liq = process_dataset(df_liq, sheet_name_liq, excel_data[sheet_name_liq])
        updated_df_sol, count_sol = process_dataset(df_sol, sheet_name_sol, excel_data[sheet_name_sol])

        if updated_df_liq is not None and updated_df_sol is not None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            xls_buffer = io.BytesIO()
            with pd.ExcelWriter(xls_buffer, engine='xlsxwriter') as writer:
                updated_df_liq.to_excel(writer, sheet_name=sheet_name_liq, index=False)
                updated_df_sol.to_excel(writer, sheet_name=sheet_name_sol, index=False)

            st.success(f"✅ Processed {count_liq} liquid and {count_sol} solid entries")
            st.download_button(
                label="📥 Download Updated Excel",
                data=xls_buffer.getvalue(),
                file_name=f"Product list 1001 food study_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    except Exception as e:
        st.error(f"❌ Error: {e}")


