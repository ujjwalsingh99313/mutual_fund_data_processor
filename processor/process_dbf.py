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

def process_cam_inv_dataframe(df, source_file="manual_upload", table_name="cam_inv_wbr2c"):
    """
    Process CAM DBF/Excel DataFrame, clean and insert/update into table.
    """
    logger.info(f"Processing data into table: {table_name} from: {source_file}")
    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    duplicate_folder = os.path.join("duplicates", today_str)
    report_folder = os.path.join("reports", today_str)
    os.makedirs(duplicate_folder, exist_ok=True)
    os.makedirs(report_folder, exist_ok=True)

    values_list, seen_keys, duplicates = [], set(), []

    # Clean & deduplicate
    for _, row in df.iterrows():
        key = (
            clean_text(row.get('folio_no')),
            clean_text(row.get('trxnno')),
            clean_date(row.get('traddate'))
        )
        cleaned_row = (
            clean_text(row.get('amc_code')), clean_text(row.get('folio_no')), clean_text(row.get('prodcode')),
            clean_text(row.get('scheme')), clean_text(row.get('inv_name')), clean_text(row.get('trxntype')),
            clean_text(row.get('trxnno')), clean_text(row.get('trxnmode')), clean_text(row.get('trxnstat')),
            clean_text(row.get('usercode')), clean_text(row.get('usrtrxno')), clean_date(row.get('traddate')),
            clean_date(row.get('postdate')), clean_numeric(row.get('purprice')), clean_numeric(row.get('units')),
            clean_numeric(row.get('amount')), clean_text(row.get('brokcode')), clean_text(row.get('subbrok')),
            clean_numeric(row.get('brokperc')), clean_numeric(row.get('brokcomm')), clean_text(row.get('altfolio')),
            clean_date(row.get('rep_date')), clean_text(row.get('time1')), clean_text(row.get('trxnsubtyp')),
            clean_numeric(row.get('applicatio')), clean_numeric(row.get('trxn_natur')), clean_text(row.get('tax')),
            clean_text(row.get('total_tax')), clean_text(row.get('te_15h')), clean_text(row.get('micr_no')),
            clean_text(row.get('remarks')), clean_text(row.get('swflag')), clean_text(row.get('old_folio')),
            clean_text(row.get('seq_no')), clean_text(row.get('reinvest_f')), clean_text(row.get('mult_brok')),
            clean_numeric(row.get('stt')), clean_text(row.get('location')), clean_text(row.get('scheme_typ')),
            clean_text(row.get('tax_status')), clean_numeric(row.get('load')), clean_text(row.get('scanrefno')),
            clean_text(row.get('pan')), clean_text(row.get('inv_iin')), clean_text(row.get('targ_src_s')),
            clean_text(row.get('trxn_type_')), clean_text(row.get('ticob_trty')), clean_text(row.get('ticob_trno')),
            clean_date(row.get('ticob_post')), clean_text(row.get('dp_id')), clean_numeric(row.get('trxn_charg')),
            clean_numeric(row.get('eligib_amt')), clean_text(row.get('src_of_txn')), clean_text(row.get('trxn_suffi')),
            clean_text(row.get('siptrxnno')), clean_text(row.get('ter_locati')), clean_text(row.get('euin')),
            clean_text(row.get('euin_valid')), clean_text(row.get('euin_opted')), clean_text(row.get('sub_brk_ar')),
            clean_text(row.get('exch_dc_fl')), clean_text(row.get('src_brk_co')), clean_text(row.get('folio_old')),
            clean_text(row.get('scheme_fol')), clean_text(row.get('amc_ref_no')), clean_numeric(row.get('stamp_duty')),
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

    # Final dedup
    unique_map = { (row[1], row[6], row[11]): row for row in values_list }
    deduped_values_list = list(unique_map.values())
    logger.info(f"Ready to insert/update: {len(deduped_values_list)} rows")

    if not deduped_values_list:
        logger.info("No valid data to insert.")
        return

    insert_query = f"""
    INSERT INTO {table_name} (
        amc_code, folio_no, prodcode, scheme, inv_name, trxntype, trxnno, trxnmode, trxnstat,
        usercode, usrtrxno, traddate, postdate, purprice, units, amount, brokcode, subbrok,
        brokperc, brokcomm, altfolio, rep_date, time1, trxnsubtyp, applicatio, trxn_natur,
        tax, total_tax, te_15h, micr_no, remarks, swflag, old_folio, seq_no, reinvest_f,
        mult_brok, stt, location, scheme_typ, tax_status, load, scanrefno, pan, inv_iin,
        targ_src_s, trxn_type_, ticob_trty, ticob_trno, ticob_post, dp_id, trxn_charg, eligib_amt,
        src_of_txn, trxn_suffi, siptrxnno, ter_locati, euin, euin_valid, euin_opted, sub_brk_ar,
        exch_dc_fl, src_brk_co, folio_old, scheme_fol, amc_ref_no, stamp_duty, file_path, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (folio_no, trxnno, traddate) DO UPDATE SET
        amount=EXCLUDED.amount, units=EXCLUDED.units, purprice=EXCLUDED.purprice,
        postdate=EXCLUDED.postdate, remarks=EXCLUDED.remarks, updated_at=EXCLUDED.updated_at
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_inserted = 0
        for batch in [deduped_values_list[i:i+BATCH_SIZE] for i in range(0, len(deduped_values_list), BATCH_SIZE)]:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_inserted += len(batch)
            logger.info(f"Inserted/updated {len(batch)} rows")

        # daily report
        report_path = os.path.join(report_folder, f"{os.path.basename(source_file).replace('.dbf','')}_inserted_or_updated.csv")
        pd.DataFrame(deduped_values_list).to_csv(report_path, index=False)

        # daily summary
        summary_path = os.path.join(report_folder, "summary.csv")
        with open(summary_path, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "inserted_or_updated", "in_file_duplicates"])
            writer.writerow([today_str, source_file, total_inserted, len(duplicates)])

    except Exception as e:
        logger.exception(f"Insert/update failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
