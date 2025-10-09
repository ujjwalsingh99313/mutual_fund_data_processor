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
    parsed = pd.to_datetime(val, errors='coerce')
    return parsed.date() if pd.notna(parsed) else None

def clean_numeric(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return float(str(val).replace(',', '').strip())
    except:
        return None

def process_kyc_status_dataframe(df, source_file="manual_upload", table_name="cam_investor_kyc_status_wbr9c"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")

    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    duplicate_folder = os.path.join("duplicates", today_str)
    report_folder = os.path.join("reports", today_str)
    os.makedirs(duplicate_folder, exist_ok=True)
    os.makedirs(report_folder, exist_ok=True)

    values_list, seen_keys, duplicates = [], set(), []

    for _, row in df.iterrows():
        key = (
            clean_text(row.get('foliochk')),
            clean_text(row.get('amc_code')),
            clean_text(row.get('product_co'))
        )

        cleaned_row = (
            clean_text(row.get('amc_code')),
            clean_text(row.get('product_co')),
            clean_text(row.get('foliochk')),
            clean_text(row.get('inv_name')),
            clean_text(row.get('address1')),
            clean_text(row.get('address2')),
            clean_text(row.get('address3')),
            clean_text(row.get('city')),
            clean_text(row.get('pincode')),
            clean_text(row.get('jnt_name1')),
            clean_text(row.get('jnt_name2')),
            clean_text(row.get('phone_off')),
            clean_text(row.get('phone_res')),
            clean_text(row.get('email')),
            clean_text(row.get('holding_na')),
            clean_text(row.get('pan_pekrn')),
            clean_text(row.get('jnt1_pan_p')),
            clean_text(row.get('jnt2_pan_p')),
            clean_text(row.get('guard_pan')),
            clean_text(row.get('tax_status')),
            clean_text(row.get('reinv_flag')),
            clean_text(row.get('bank_name')),
            clean_text(row.get('branch')),
            clean_text(row.get('ac_type')),
            clean_text(row.get('ac_no')),
            clean_text(row.get('ifsc_code')),
            clean_text(row.get('b_address1')),
            clean_text(row.get('b_address2')),
            clean_text(row.get('b_address3')),
            clean_text(row.get('b_city')),
            clean_text(row.get('b_pincode')),
            clean_date(row.get('inv_dob')),
            clean_text(row.get('mobile_no')),
            clean_text(row.get('occupation')),
            clean_text(row.get('inv_iin')),
            clean_text(row.get('nom_optout')),
            clean_text(row.get('nom_name')),
            clean_text(row.get('relation')),
            clean_numeric(row.get('nom_percen')),
            clean_text(row.get('nom2_name')),
            clean_text(row.get('nom2_relat')),
            clean_numeric(row.get('nom2_perce')),
            clean_text(row.get('nom3_name')),
            clean_text(row.get('nom3_relat')),
            clean_numeric(row.get('nom3_perce')),
            clean_text(row.get('guard_name')),
            clean_text(row.get('guardian_r')),
            clean_date(row.get('folio_date')),
            clean_text(row.get('fh_ckyc_no')),
            clean_text(row.get('jh1_ckyc')),
            clean_text(row.get('jh2_ckyc')),
            clean_text(row.get('g_ckyc_no')),
            clean_date(row.get('jh1_dob')),
            clean_date(row.get('jh2_dob')),
            clean_date(row.get('guardian_d')),
            clean_text(row.get('gst_state_')),
            clean_text(row.get('folio_old')),
            clean_text(row.get('scheme_fol')),
            clean_text(row.get('jh1_mobile')),
            clean_text(row.get('jh1_email')),
            clean_text(row.get('jh2_mobile')),
            clean_text(row.get('jh2_email')),
            clean_text(row.get('fh_mobile_')),
            clean_text(row.get('fh_email_f')),
            clean_text(row.get('jh1_mobile_')),
            clean_text(row.get('jh1_email_')),
            clean_text(row.get('jh2_mobile_')),
            clean_text(row.get('jh2_email_')),
            clean_text(row.get('fh_kyc')),
            clean_text(row.get('gu_kyc')),
            clean_text(row.get('jh1_kyc')),
            clean_text(row.get('jh2_kyc')),
            clean_text(row.get('fh_g_aadha')),
            clean_text(row.get('jh1_aadhar')),
            clean_text(row.get('jh2_aadhar')),
            clean_text(row.get('dp_id')),
            clean_text(row.get('demat')),
            clean_numeric(row.get('clos_bal')),
            clean_numeric(row.get('rupee_bal')),
            clean_text(row.get('country')),
            clean_text(row.get('nom_pan')),
            clean_date(row.get('nom_dob')),
            clean_text(row.get('nom_guardi')),
            clean_text(row.get('nom2_pan')),
            clean_date(row.get('nom2_dob')),
            clean_text(row.get('nom2_guard')),
            clean_text(row.get('nom3_pan')),
            clean_date(row.get('nom3_dob')),
            clean_text(row.get('nom3_guard')),
            clean_text(row.get('can')),
            clean_text(row.get('fh_guardia')),
            clean_text(row.get('jh1_valid_')),
            clean_text(row.get('jh2_valid_')),
            source_file,
            datetime.now(),
            datetime.now()
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

    if not values_list:
        logger.info("No valid data to insert.")
        return

    insert_query = f"""
    INSERT INTO {table_name} (
        amc_code, product_co, foliochk, inv_name, address1, address2, address3, city, pincode,
        jnt_name1, jnt_name2, phone_off, phone_res, email, holding_na, pan_pekrn, jnt1_pan_p,
        jnt2_pan_p, guard_pan, tax_status, reinv_flag, bank_name, branch, ac_type, ac_no, ifsc_code,
        b_address1, b_address2, b_address3, b_city, b_pincode, inv_dob, mobile_no, occupation,
        inv_iin, nom_optout, nom_name, relation, nom_percen, nom2_name, nom2_relat, nom2_perce,
        nom3_name, nom3_relat, nom3_perce, guard_name, guardian_r, folio_date, fh_ckyc_no, jh1_ckyc,
        jh2_ckyc, g_ckyc_no, jh1_dob, jh2_dob, guardian_d, gst_state_, folio_old, scheme_fol,
        jh1_mobile, jh1_email, jh2_mobile, jh2_email, fh_mobile_, fh_email_f, jh1_mobile_, jh1_email_,
        jh2_mobile_, jh2_email_, fh_kyc, gu_kyc, jh1_kyc, jh2_kyc, fh_g_aadha, jh1_aadhar, jh2_aadhar,
        dp_id, demat, clos_bal, rupee_bal, country, nom_pan, nom_dob, nom_guardi, nom2_pan, nom2_dob,
        nom2_guard, nom3_pan, nom3_dob, nom3_guard, can, fh_guardia, jh1_valid_, jh2_valid_,
        file_path, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (foliochk, amc_code, product_co) DO UPDATE SET
        inv_name=EXCLUDED.inv_name, address1=EXCLUDED.address1, address2=EXCLUDED.address2,
        address3=EXCLUDED.address3, city=EXCLUDED.city, pincode=EXCLUDED.pincode,
        pan_pekrn=EXCLUDED.pan_pekrn, updated_at=EXCLUDED.updated_at
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_inserted = 0
        for batch in [values_list[i:i+BATCH_SIZE] for i in range(0, len(values_list), BATCH_SIZE)]:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_inserted += len(batch)
            logger.info(f"Inserted/updated {len(batch)} rows")

        # daily report
        report_path = os.path.join(report_folder, f"{os.path.basename(source_file).replace('.dbf','')}_inserted_or_updated.csv")
        pd.DataFrame(values_list).to_csv(report_path, index=False)

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
