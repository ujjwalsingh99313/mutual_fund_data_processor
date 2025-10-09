import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from config.db_config import get_db_connection
from utils.logger import logger
import os
import re
import csv
import hashlib

BATCH_SIZE = 500

def to_snake_case(name):
    name = re.sub(r'[\s]+', '_', name.strip())
    name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', name)
    return name.lower()

def clean_text(val):
    return str(val).strip() if pd.notna(val) and str(val).strip() else None

def clean_date(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    parsed = pd.to_datetime(val, errors='coerce')
    return parsed.date() if pd.notna(parsed) else None

def clean_numeric(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return float(str(val).replace(',', '').strip())
    except:
        return None

def clean_integer(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return int(str(val).replace(',', '').strip())
    except:
        return None

def get_file_hash(file_path):
    """Generate MD5 hash of file to detect if it was already processed"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def is_file_already_processed(file_hash, table_name):
    """Check if file was already processed by looking in file_tracking table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 1 FROM file_processing_tracker 
            WHERE file_hash = %s AND table_name = %s AND status = 'COMPLETED'
        """, (file_hash, table_name))
        return cursor.fetchone() is not None
    except Exception as e:
        # If table doesn't exist, assume file wasn't processed
        logger.warning(f"File tracking check failed: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def mark_file_as_processed(file_hash, table_name, source_file, total_records):
    """Mark file as processed in tracking table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Create tracking table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_processing_tracker (
                id SERIAL PRIMARY KEY,
                file_hash VARCHAR(64) UNIQUE NOT NULL,
                table_name VARCHAR(100) NOT NULL,
                source_file VARCHAR(500) NOT NULL,
                total_records INTEGER NOT NULL,
                status VARCHAR(20) NOT NULL,
                processed_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        cursor.execute("""
            INSERT INTO file_processing_tracker 
            (file_hash, table_name, source_file, total_records, status)
            VALUES (%s, %s, %s, %s, 'COMPLETED')
            ON CONFLICT (file_hash) DO UPDATE SET
                table_name = EXCLUDED.table_name,
                source_file = EXCLUDED.source_file,
                total_records = EXCLUDED.total_records,
                status = EXCLUDED.status,
                processed_at = NOW()
        """, (file_hash, table_name, source_file, total_records))
        
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to mark file as processed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_cam_brokerage_dataframe(df, source_file="manual_upload", table_name="cam_brokerage_wbr77"):
    """Process brokerage data WITHOUT any duplication checks"""
    
    # Check if file was already processed (if source_file is actual file path)
    if os.path.exists(source_file):
        file_hash = get_file_hash(source_file)
        if is_file_already_processed(file_hash, table_name):
            logger.info(f"File already processed: {source_file}")
            return 0  # File was already processed
    
    logger.info(f"Processing brokerage data into table: {table_name} from: {source_file}")

    # Convert column names to snake_case
    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Create folders for reports
    report_folder = os.path.join("reports", today_str)
    os.makedirs(report_folder, exist_ok=True)

    values_list = []

    for _, row in df.iterrows():
        # Clean all fields according to their data types
        cleaned_row = (
            clean_text(row.get('amc_code')),
            clean_text(row.get('proc_date')),
            clean_text(row.get('folio_no')),
            clean_text(row.get('scheme_cod')),
            clean_text(row.get('trxn_type')),
            clean_text(row.get('trxn_no')),
            clean_numeric(row.get('plot_amoun')),
            clean_numeric(row.get('plot_units')),
            clean_text(row.get('post_date')),
            clean_text(row.get('trade_date')),
            clean_text(row.get('entry_date')),
            clean_text(row.get('user_code')),
            clean_text(row.get('user_trxnn')),
            clean_text(row.get('trxn_natur')),
            clean_text(row.get('ter_locati')),
            clean_text(row.get('sys_reg_da')),
            clean_text(row.get('aut_txn_no')),
            clean_numeric(row.get('auto_amoun')),
            clean_text(row.get('aut_txn_ty')),
            clean_text(row.get('cease_date')),
            clean_text(row.get('remed_date')),
            clean_text(row.get('forf_date')),
            clean_text(row.get('src_brk_co')),
            clean_text(row.get('brok_code')),
            clean_text(row.get('brh_code')),
            clean_text(row.get('sub_brk_ar')),
            clean_text(row.get('ae_code')),
            clean_text(row.get('arn_emp_co')),
            clean_text(row.get('euin_opted')),
            clean_text(row.get('euin_valid')),
            clean_text(row.get('brk_comm_p')),
            clean_text(row.get('adj_flag')),
            clean_text(row.get('brkage_typ')),
            clean_numeric(row.get('brkage_rat')),
            clean_numeric(row.get('total_upfr')),
            clean_text(row.get('defer_freq')),
            clean_integer(row.get('defer_no_o')),
            clean_integer(row.get('pay_instal')),
            clean_numeric(row.get('brkage_amt')),
            clean_text(row.get('brkage_fro')),
            clean_text(row.get('brkage_to')),
            clean_text(row.get('proc_from_')),
            clean_text(row.get('proc_to_da')),
            clean_text(row.get('trxn_desc')),
            clean_integer(row.get('spl_upf_te')),
            clean_text(row.get('upf_tenure')),
            clean_text(row.get('brk_pay_dt')),
            clean_text(row.get('clw_type')),
            clean_integer(row.get('clw_period')),
            clean_text(row.get('rec_flag')),
            clean_text(row.get('p_si_date')),
            clean_integer(row.get('rec_period')),
            clean_numeric(row.get('clw_amt')),
            clean_numeric(row.get('upf_paid')),
            clean_integer(row.get('fee_id')),
            clean_text(row.get('am_code')),
            clean_numeric(row.get('am_comm')),
            clean_numeric(row.get('am_rate')),
            clean_numeric(row.get('avg_assets')),
            clean_numeric(row.get('cam_comm')),
            clean_numeric(row.get('cam_rate')),
            clean_numeric(row.get('mam_comm')),
            clean_numeric(row.get('mam_rate')),
            clean_integer(row.get('no_of_days')),
            clean_text(row.get('orig_ae_co')),
            clean_text(row.get('orig_brh_c')),
            clean_text(row.get('orig_brk_c')),
            clean_text(row.get('rate_ref_i')),
            clean_text(row.get('ref_no')),
            clean_text(row.get('trxn_app_n')),
            clean_text(row.get('txn_sch_co')),
            clean_integer(row.get('clw_prd')),
            clean_text(row.get('clw_requir')),
            clean_text(row.get('p_si_mis_c')),
            clean_integer(row.get('p_si_user_')),
            clean_integer(row.get('seq_no')),
            clean_numeric(row.get('p_si_amt')),
            clean_integer(row.get('p_si_tr_no')),
            clean_text(row.get('p_si_type')),
            clean_numeric(row.get('pur_si_uni')),
            clean_text(row.get('remarks')),
            clean_text(row.get('to_scheme')),
            clean_text(row.get('trxn_sign')),
            clean_text(row.get('brk_posted')),
            clean_text(row.get('inv_name')),
            clean_text(row.get('brok_gst_s')),
            clean_numeric(row.get('igst_rate')),
            clean_numeric(row.get('cgst_rate')),
            clean_numeric(row.get('sgst_rate')),
            clean_numeric(row.get('igst_value')),
            clean_numeric(row.get('cgst_value')),
            clean_numeric(row.get('sgst_value')),
            clean_text(row.get('location_c')),
            clean_text(row.get('prev_folio')),
            clean_text(row.get('brok_categ')),
            clean_text(row.get('p_scheme_c')),
            clean_text(row.get('p_trxn_typ')),
            clean_text(row.get('p_trxn_no')),
            clean_text(row.get('p_folio_no')),
            clean_numeric(row.get('p_plot_amo')),
            clean_numeric(row.get('p_plot_uni')),
            clean_text(row.get('folio_old')),
            clean_text(row.get('scheme_fol')),
            clean_text(row.get('amc_ref_no')),
            clean_text(row.get('request_re')),
            clean_text(row.get('write_off_')),
            clean_text(row.get('hold_reaso')),
            clean_text(row.get('brokerage_')),
            source_file,
            datetime.now(),
            datetime.now()
        )

        values_list.append(cleaned_row)

    logger.info(f"Total rows to process: {len(values_list)}")

    if not values_list:
        logger.info("No data to insert.")
        return 0

    # SIMPLE INSERT WITHOUT ANY CONFLICT HANDLING - Saara data insert hoga
    insert_query = f"""
    INSERT INTO {table_name} (
        amc_code, proc_date, folio_no, scheme_cod, trxn_type, trxn_no, plot_amoun, plot_units, 
        post_date, trade_date, entry_date, user_code, user_trxnn, trxn_natur, ter_locati, 
        sys_reg_da, aut_txn_no, auto_amoun, aut_txn_ty, cease_date, remed_date, forf_date, 
        src_brk_co, brok_code, brh_code, sub_brk_ar, ae_code, arn_emp_co, euin_opted, 
        euin_valid, brk_comm_p, adj_flag, brkage_typ, brkage_rat, total_upfr, defer_freq, 
        defer_no_o, pay_instal, brkage_amt, brkage_fro, brkage_to, proc_from_, proc_to_da, 
        trxn_desc, spl_upf_te, upf_tenure, brk_pay_dt, clw_type, clw_period, rec_flag, 
        p_si_date, rec_period, clw_amt, upf_paid, fee_id, am_code, am_comm, am_rate, 
        avg_assets, cam_comm, cam_rate, mam_comm, mam_rate, no_of_days, orig_ae_co, 
        orig_brh_c, orig_brk_c, rate_ref_i, ref_no, trxn_app_n, txn_sch_co, clw_prd, 
        clw_requir, p_si_mis_c, p_si_user_, seq_no, p_si_amt, p_si_tr_no, p_si_type, 
        pur_si_uni, remarks, to_scheme, trxn_sign, brk_posted, inv_name, brok_gst_s, 
        igst_rate, cgst_rate, sgst_rate, igst_value, cgst_value, sgst_value, location_c, 
        prev_folio, brok_categ, p_scheme_c, p_trxn_typ, p_trxn_no, p_folio_no, p_plot_amo, 
        p_plot_uni, folio_old, scheme_fol, amc_ref_no, request_re, write_off_, hold_reaso, 
        brokerage_, file_path, created_at, updated_at
    ) VALUES %s
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_processed = 0
        for batch in [values_list[i:i+BATCH_SIZE] for i in range(0, len(values_list), BATCH_SIZE)]:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_processed += len(batch)  # Saare rows insert hue hain
            logger.info(f"Inserted {len(batch)} rows from batch of {len(batch)}")

        # Mark file as processed if it's a physical file
        if os.path.exists(source_file):
            mark_file_as_processed(
                file_hash=get_file_hash(source_file),
                table_name=table_name,
                source_file=source_file,
                total_records=total_processed
            )

        # Save processed data to report file
        report_file = os.path.join(report_folder, f"{os.path.basename(source_file)}_inserted.csv")
        pd.DataFrame(values_list).to_csv(report_file, index=False)

        # Update summary file
        summary_file = os.path.join(report_folder, "summary.csv")
        with open(summary_file, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "inserted", "total_records"])
            writer.writerow([today_str, source_file, total_processed, len(values_list)])

        logger.info(f"Total inserted: {total_processed} out of {len(values_list)} records")
        return total_processed

    except Exception as e:
        logger.exception(f"Insert failed: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()