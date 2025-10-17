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
    try:
        parsed = pd.to_datetime(val, errors='coerce')
        return parsed.date() if pd.notna(parsed) else None
    except:
        return None

def clean_numeric(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return float(str(val).replace(',', '').strip())
    except:
        return None

def get_file_hash(file_path):
    """Generate MD5 hash of file content to detect duplicate files"""
    if not os.path.exists(file_path):
        return None
    
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def is_file_already_processed(file_hash, table_name="cam_sip_wbr49"):
    """Check if file has been processed before"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 1 FROM processed_files 
            WHERE file_hash = %s AND table_name = %s
        """, (file_hash, table_name))
        return cursor.fetchone() is not None
    except Exception as e:
        # If processed_files table doesn't exist, create it
        if "relation \"processed_files\" does not exist" in str(e):
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(file_hash, table_name)
                )
            """)
            conn.commit()
        return False
    finally:
        cursor.close()
        conn.close()

def mark_file_as_processed(file_name, file_hash, table_name="cam_sip_wbr49"):
    """Mark file as processed to prevent re-processing"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO processed_files (file_name, file_hash, table_name)
            VALUES (%s, %s, %s)
        """, (file_name, file_hash, table_name))
        conn.commit()
    except Exception as e:
        logger.error(f"Error marking file as processed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_sip_wbr49_dataframe(df, source_file="manual_upload", table_name="cam_sip_wbr49"):
    """
    Process SIP WBR49 DataFrame dynamically into table_name with duplicate file prevention.
    """
    logger.info(f"Processing data into table: {table_name} from: {source_file}")
    
    # Check if file has been processed before (for actual file paths)
    if source_file != "manual_upload" and os.path.exists(source_file):
        file_hash = get_file_hash(source_file)
        if file_hash and is_file_already_processed(file_hash, table_name):
            logger.warning(f"File {source_file} has already been processed. Skipping.")
            return {"status": "skipped", "reason": "file_already_processed"}
    
    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    duplicate_folder = os.path.join("duplicates", today_str)
    report_folder = os.path.join("reports", today_str)
    os.makedirs(duplicate_folder, exist_ok=True)
    os.makedirs(report_folder, exist_ok=True)

    values_list, seen_keys, duplicates = [], set(), []

    # Deduplicate based on composite key only (remove request_re from dedup key)
    for _, row in df.iterrows():
        key = (
            clean_text(row.get('folio_no')),
            clean_numeric(row.get('auto_trno')),
            clean_date(row.get('from_date'))
            # Removed request_re from deduplication key
        )
        cleaned_row = (
            clean_text(row.get('product')), clean_text(row.get('scheme')), clean_text(row.get('folio_no')),
            clean_text(row.get('inv_name')), clean_text(row.get('aut_trntyp')), clean_numeric(row.get('auto_trno')),
            clean_numeric(row.get('auto_amoun')), clean_date(row.get('from_date')), clean_date(row.get('to_date')),
            clean_date(row.get('cease_date')), clean_text(row.get('periodicit')), clean_numeric(row.get('period_day')),
            clean_text(row.get('inv_iin')), clean_text(row.get('payment_mo')), clean_text(row.get('target_sch')),
            clean_date(row.get('reg_date')), clean_text(row.get('subbroker')), clean_text(row.get('remarks')),
            clean_text(row.get('top_up_frq')), clean_numeric(row.get('top_up_amt')), clean_text(row.get('ac_type')),
            clean_text(row.get('bank')), clean_text(row.get('branch')), clean_text(row.get('instrm_no')),
            clean_text(row.get('cheq_micr_')), clean_text(row.get('ac_holder_')), clean_text(row.get('pan')),
            clean_text(row.get('top_up_per')), clean_text(row.get('euin')), clean_text(row.get('sub_arn_co')),
            clean_text(row.get('ter_locati')), clean_text(row.get('scheme_cod')), clean_text(row.get('target_sch_2')),
            clean_text(row.get('amc_code')), clean_text(row.get('user_code')), clean_text(row.get('package_na')),
            clean_text(row.get('special_pr')), clean_text(row.get('subtrxndes')), clean_date(row.get('pause_from')),
            clean_date(row.get('pause_to_d')), clean_text(row.get('folio_old')), clean_text(row.get('ft_sip_reg')),
            clean_text(row.get('scheme_fol')), clean_text(row.get('request_re')),
            source_file, datetime.now(), datetime.now()
        )
        if key in seen_keys:
            duplicates.append(row)
        else:
            seen_keys.add(key)
            values_list.append(cleaned_row)

    logger.info(f"Original rows: {len(df)}, unique after in-file deduplication: {len(values_list)}, duplicates: {len(duplicates)}")

    if duplicates:
        dup_file = os.path.join(duplicate_folder, f"{os.path.basename(source_file).replace('.dbf','')}_duplicates.csv")
        pd.DataFrame(duplicates).to_csv(dup_file, index=False)
        logger.warning(f"Saved in-file duplicates: {dup_file}")

    # Final dedup - use only composite key
    unique_map = { (row[2], row[5], row[7]): row for row in values_list }  # folio_no, auto_trno, from_date only
    deduped_values_list = list(unique_map.values())
    logger.info(f"Ready to insert/update: {len(deduped_values_list)} rows")

    if not deduped_values_list:
        logger.info("No valid data to insert.")
        return {"status": "skipped", "reason": "no_valid_data"}

    # CHANGED: Use composite key conflict resolution instead of request_re
    insert_query = f"""
    INSERT INTO {table_name} (
        product, scheme, folio_no, inv_name, aut_trntyp, auto_trno, auto_amoun, from_date, to_date, cease_date,
        periodicit, period_day, inv_iin, payment_mo, target_sch, reg_date, subbroker, remarks, top_up_frq, top_up_amt,
        ac_type, bank, branch, instrm_no, cheq_micr_, ac_holder_, pan, top_up_per, euin, sub_arn_co, ter_locati,
        scheme_cod, target_sch_2, amc_code, user_code, package_na, special_pr, subtrxndes, pause_from, pause_to_d,
        folio_old, ft_sip_reg, scheme_fol, request_re, file_path, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (folio_no, auto_trno, from_date) DO UPDATE SET
        product=EXCLUDED.product,
        scheme=EXCLUDED.scheme,
        inv_name=EXCLUDED.inv_name,
        aut_trntyp=EXCLUDED.aut_trntyp,
        auto_amoun=EXCLUDED.auto_amoun,
        to_date=EXCLUDED.to_date,
        cease_date=EXCLUDED.cease_date,
        periodicit=EXCLUDED.periodicit,
        period_day=EXCLUDED.period_day,
        inv_iin=EXCLUDED.inv_iin,
        payment_mo=EXCLUDED.payment_mo,
        target_sch=EXCLUDED.target_sch,
        reg_date=EXCLUDED.reg_date,
        subbroker=EXCLUDED.subbroker,
        remarks=EXCLUDED.remarks,
        top_up_frq=EXCLUDED.top_up_frq,
        top_up_amt=EXCLUDED.top_up_amt,
        ac_type=EXCLUDED.ac_type,
        bank=EXCLUDED.bank,
        branch=EXCLUDED.branch,
        instrm_no=EXCLUDED.instrm_no,
        cheq_micr_=EXCLUDED.cheq_micr_,
        ac_holder_=EXCLUDED.ac_holder_,
        pan=EXCLUDED.pan,
        top_up_per=EXCLUDED.top_up_per,
        euin=EXCLUDED.euin,
        sub_arn_co=EXCLUDED.sub_arn_co,
        ter_locati=EXCLUDED.ter_locati,
        scheme_cod=EXCLUDED.scheme_cod,
        target_sch_2=EXCLUDED.target_sch_2,
        amc_code=EXCLUDED.amc_code,
        user_code=EXCLUDED.user_code,
        package_na=EXCLUDED.package_na,
        special_pr=EXCLUDED.special_pr,
        subtrxndes=EXCLUDED.subtrxndes,
        pause_from=EXCLUDED.pause_from,
        pause_to_d=EXCLUDED.pause_to_d,
        folio_old=EXCLUDED.folio_old,
        ft_sip_reg=EXCLUDED.ft_sip_reg,
        scheme_fol=EXCLUDED.scheme_fol,
        request_re=EXCLUDED.request_re,
        file_path=EXCLUDED.file_path,
        updated_at=EXCLUDED.updated_at
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_inserted = 0
        for batch in [deduped_values_list[i:i+BATCH_SIZE] for i in range(0, len(deduped_values_list), BATCH_SIZE)]:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_inserted += len(batch)  
            logger.info(f"Processed {len(batch)} rows")

    
        if source_file != "manual_upload" and os.path.exists(source_file):
            file_hash = get_file_hash(source_file)
            if file_hash:
                mark_file_as_processed(os.path.basename(source_file), file_hash, table_name)

      
        report_path = os.path.join(report_folder, f"{os.path.basename(source_file).replace('.dbf','')}_inserted_or_updated.csv")
        pd.DataFrame(deduped_values_list).to_csv(report_path, index=False)

        summary_path = os.path.join(report_folder, "summary.csv")
        with open(summary_path, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "processed_rows", "in_file_duplicates"])
            writer.writerow([today_str, source_file, total_inserted, len(duplicates)])

        logger.info(f"Processing complete: {total_inserted} rows processed")
        return {"status": "success", "processed": total_inserted, "duplicates": len(duplicates)}
        
    except Exception as e:
        logger.exception(f"Insert/update failed: {e}")
        conn.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        cursor.close()
        conn.close()