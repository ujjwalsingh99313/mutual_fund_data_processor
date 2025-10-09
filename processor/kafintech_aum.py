import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from config.db_config import get_db_connection
from utils.logger import logger
import os
import re
import csv

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

def process_kfintech_aum_dataframe(df, source_file="manual_upload", table_name="kfintech_client_aum"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")

    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    base_folder = os.path.join("kfintech_reports", today_str)
    os.makedirs(base_folder, exist_ok=True)

    values_list, seen_keys, duplicates = [], set(), []

    for _, row in df.iterrows():
        key = (
            clean_text(row.get('acno')),
            clean_text(row.get('scheme')),
            clean_date(row.get('todate'))
        )

        cleaned_row = (
            clean_text(row.get('prcode')), clean_text(row.get('fund')), clean_text(row.get('acno')),
            clean_text(row.get('scheme')), clean_text(row.get('funddesc')), clean_numeric(row.get('balunits')),
            clean_numeric(row.get('pldg')), clean_date(row.get('trdate')), clean_text(row.get('trdesc')),
            clean_text(row.get('moh')), clean_text(row.get('brokcode')), clean_text(row.get('sbcode')),
            clean_text(row.get('pout')), clean_text(row.get('inv_id')), clean_text(row.get('invname')),
            clean_text(row.get('add1')), clean_text(row.get('add2')), clean_text(row.get('add3')),
            clean_text(row.get('city')), clean_text(row.get('inv_pin')), clean_text(row.get('rphone')),
            clean_text(row.get('ophone')), clean_text(row.get('fax')), clean_text(row.get('email')),
            clean_numeric(row.get('valinv')), clean_numeric(row.get('lnav')), clean_date(row.get('crdate')),
            clean_text(row.get('crtime')), clean_text(row.get('divopt')), clean_text(row.get('pldgbank')),
            clean_text(row.get('pan')), clean_text(row.get('pln')), clean_text(row.get('schemeisin')),
            clean_text(row.get('client_id')), clean_text(row.get('dp_id')), clean_date(row.get('todate')),
            clean_text(row.get('mobile')), source_file, datetime.now(), datetime.now()
        )

        if key in seen_keys:
            duplicates.append(row)
        else:
            seen_keys.add(key)
            values_list.append(cleaned_row)

    logger.info(f"Original rows: {len(df)}, unique after in-file deduplication: {len(values_list)}, duplicates: {len(duplicates)}")

    if duplicates:
        dup_file = os.path.join(base_folder, f"{os.path.basename(source_file)}_duplicates.csv")
        pd.DataFrame(duplicates).to_csv(dup_file, index=False)
        logger.warning(f"Saved in-file duplicates: {dup_file}")

    unique_map = {(row[2], row[3], row[35]): row for row in values_list}
    deduped_values_list = list(unique_map.values())

    logger.info(f"Ready to insert/update: {len(deduped_values_list)} rows")

    if not deduped_values_list:
        logger.info("No valid data to insert.")
        return 0  
    insert_query = f"""
    INSERT INTO {table_name} (
        prcode, fund, acno, scheme, funddesc, balunits, pldg, trdate, trdesc, moh, brokcode, sbcode, pout, inv_id, invname,
        add1, add2, add3, city, inv_pin, rphone, ophone, fax, email, valinv, lnav, crdate, crtime, divopt, pldgbank, pan,
        pln, schemeisin, client_id, dp_id, todate, mobile, file_path, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (acno, scheme, todate) DO UPDATE SET
        balunits=EXCLUDED.balunits, valinv=EXCLUDED.valinv, lnav=EXCLUDED.lnav, trdate=EXCLUDED.trdate,
        updated_at=EXCLUDED.updated_at
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    total_inserted = 0
    try:
        for batch in [deduped_values_list[i:i+BATCH_SIZE] for i in range(0, len(deduped_values_list), BATCH_SIZE)]:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_inserted += len(batch)
            logger.info(f"Inserted/updated {len(batch)} rows")

        report_path = os.path.join(base_folder, f"{os.path.basename(source_file)}_inserted_or_updated.csv")
        pd.DataFrame(deduped_values_list).to_csv(report_path, index=False)

        summary_path = os.path.join(base_folder, "summary.csv")
        with open(summary_path, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "inserted_or_updated", "in_file_duplicates"])
            writer.writerow([today_str, source_file, total_inserted, len(duplicates)])

        logger.info(f"Total inserted/updated: {total_inserted}")
        
        return total_inserted  
    except Exception as e:
        logger.exception(f"Insert/update failed: {e}")
        conn.rollback()
        return 0  
    finally:
        cursor.close()
        conn.close()