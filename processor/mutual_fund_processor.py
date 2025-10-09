import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from config.db_config import get_db_connection
from utils.logger import logger
import os
import shutil

BATCH_SIZE = 500

def clean_text(val):
    return str(val).strip() if pd.notna(val) and str(val).strip() else None

def clean_date(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        parsed = pd.to_datetime(val, errors='coerce')
        return parsed.date() if pd.notna(parsed) else None
    except:
        return None

def clean_flag(val):
    val = str(val).strip().upper()
    return val if val in ('Y', 'N') else 'N'

def clean_numeric(val, col=''):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        val_str = str(val).replace(',', '').replace('%', '').strip()
        return float(val_str) if '.' in val_str else int(val_str)
    except:
        logger.warning(f"[Column: {col}] Cannot convert '{val}' to number")
        return None

def prepare_data(df, sheet_name):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
    if 'sl_no' in df.columns:
        df.drop(columns=['sl_no'], inplace=True)
    return df.drop_duplicates(subset=['scheme_code', 'plan_name', 'plan_type'], keep='last')

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def archive_file(file_path):
    today_str = datetime.now().strftime('%Y-%m-%d')
    archive_path = os.path.join('archive', today_str)
    os.makedirs(archive_path, exist_ok=True)
    shutil.move(file_path, os.path.join(archive_path, os.path.basename(file_path)))
    logger.info(f"Archived file to: {archive_path}")

def save_excel_report(df, folder_name, file_name):
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_path = os.path.join(folder_name, today_str)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, file_name)
    df.to_excel(file_path, index=False)
    logger.info(f"Saved report: {file_path}")

def fetch_existing_keys(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT scheme_code, plan_name, plan_type FROM {table_name}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return set((r[0], r[1], r[2]) for r in rows)

def insert_dataframe(df, table_name, file_path):
    existing_keys = fetch_existing_keys(table_name)
    values_list = []
    updated_rows, inserted_rows = [], []

    for _, row in df.iterrows():
        key = (
            clean_text(row.get('scheme_code')),
            clean_text(row.get('plan_name')),
            clean_text(row.get('plan_type'))
        )
        if None in key:
            continue

        row_data = (
            key[0],
            clean_text(row.get('fund_code')),
            key[1],
            clean_text(row.get('scheme_type')),
            key[2],
            clean_text(row.get('plan_opt')),
            clean_text(row.get('div_opt')),
            clean_text(row.get('amfi_id')),
            clean_text(row.get('pri_isin')),
            clean_text(row.get('sec_isin')),
            clean_date(row.get('nfo_start')),
            clean_date(row.get('nfo_end')),
            clean_date(row.get('allot_date')),
            clean_date(row.get('reopen_date')),
            clean_date(row.get('maturity_date')),
            clean_text(row.get('entry_load')),
            clean_text(row.get('exit_load')),
            clean_flag(row.get('pur_allowed')),
            clean_flag(row.get('nfo_allowed')),
            clean_flag(row.get('redeem_allowed')),
            clean_flag(row.get('sip_allowed')),
            clean_flag(row.get('switch_out_allowed')),
            clean_flag(row.get('switch_in_allowed')),
            clean_flag(row.get('stp_out_allowed')),
            clean_flag(row.get('stp_in_allowed')),
            clean_flag(row.get('swp_allowed')),
            clean_flag(row.get('demat_allowed')),
            clean_numeric(row.get('catg_id'), 'catg_id'),
            clean_numeric(row.get('sub_catg_id'), 'sub_catg_id'),
            clean_text(row.get('scheme_flag')),
            clean_text(file_path),
            datetime.now(),
            datetime.now()
        )

        values_list.append(row_data)

        if key in existing_keys:
            updated_rows.append(row)
        else:
            inserted_rows.append(row)

    if updated_rows:
        save_excel_report(pd.DataFrame(updated_rows), 'reports', 'updated.xlsx')
    if inserted_rows:
        save_excel_report(pd.DataFrame(inserted_rows), 'reports', 'inserted.xlsx')

    # SQL insert with ON CONFLICT update
    upsert_query = f"""
    INSERT INTO {table_name} (
        scheme_code, fund_code, plan_name, scheme_type, plan_type, plan_opt,
        div_opt, amfi_id, pri_isin, sec_isin, nfo_start, nfo_end,
        allot_date, reopen_date, maturity_date, entry_load, exit_load,
        pur_allowed, nfo_allowed, redeem_allowed, sip_allowed, switch_out_allowed,
        switch_in_allowed, stp_out_allowed, stp_in_allowed, swp_allowed, demat_allowed,
        catg_id, sub_catg_id, scheme_flag, file_path, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (scheme_code, plan_name, plan_type) DO UPDATE SET
        fund_code = EXCLUDED.fund_code,
        scheme_type = EXCLUDED.scheme_type,
        plan_opt = EXCLUDED.plan_opt,
        div_opt = EXCLUDED.div_opt,
        amfi_id = EXCLUDED.amfi_id,
        pri_isin = EXCLUDED.pri_isin,
        sec_isin = EXCLUDED.sec_isin,
        nfo_start = EXCLUDED.nfo_start,
        nfo_end = EXCLUDED.nfo_end,
        allot_date = EXCLUDED.allot_date,
        reopen_date = EXCLUDED.reopen_date,
        maturity_date = EXCLUDED.maturity_date,
        entry_load = EXCLUDED.entry_load,
        exit_load = EXCLUDED.exit_load,
        pur_allowed = EXCLUDED.pur_allowed,
        nfo_allowed = EXCLUDED.nfo_allowed,
        redeem_allowed = EXCLUDED.redeem_allowed,
        sip_allowed = EXCLUDED.sip_allowed,
        switch_out_allowed = EXCLUDED.switch_out_allowed,
        switch_in_allowed = EXCLUDED.switch_in_allowed,
        stp_out_allowed = EXCLUDED.stp_out_allowed,
        stp_in_allowed = EXCLUDED.stp_in_allowed,
        swp_allowed = EXCLUDED.swp_allowed,
        demat_allowed = EXCLUDED.demat_allowed,
        catg_id = EXCLUDED.catg_id,
        sub_catg_id = EXCLUDED.sub_catg_id,
        scheme_flag = EXCLUDED.scheme_flag,
        file_path = EXCLUDED.file_path,
        updated_at = EXCLUDED.updated_at
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    total = 0
    try:
        for batch in chunked(values_list, BATCH_SIZE):
            execute_values(cursor, upsert_query, batch)
            conn.commit()
            total += len(batch)
            logger.info(f"Inserted batch. Total processed so far: {total}")
    except Exception as e:
        logger.exception(f"Insert failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_combined_excel(file_path):
    table_name = 'mutual_funds'
    logger.info(f" Starting processing: {file_path}")

    combined_df = pd.DataFrame()

    try:
        if file_path.endswith('.csv'): 
            df = pd.read_csv(file_path, dtype=str)
            df = prepare_data(df, 'CSV')
            combined_df = df
        elif file_path.endswith(('.xlsx', '.xls')):
            with pd.ExcelFile(file_path, engine='openpyxl' if file_path.endswith('xlsx') else 'xlrd') as excel_file:
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str)
                    df = prepare_data(df, sheet_name)
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
        else:
            logger.error(" Unsupported file format")
            return
    except Exception as e:
        logger.exception(f" Failed to read file: {e}")
        return

    # Remove true duplicates (based on business key)
    dup_df = combined_df[combined_df.duplicated(subset=['scheme_code', 'plan_name', 'plan_type'], keep=False)]
    if not dup_df.empty:
        save_excel_report(dup_df, 'reports', 'duplicates.xlsx')

    unique_df = combined_df.drop_duplicates(subset=['scheme_code', 'plan_name', 'plan_type'], keep='last')
    logger.info(f" Unique rows to insert/update: {len(unique_df)}")

    insert_dataframe(unique_df, table_name, file_path)
    archive_file(file_path)
