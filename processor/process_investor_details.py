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

def process_investor_details_dataframe(df, source_file="manual_upload", table_name="cam_investor_details_wbr9"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")

    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    duplicate_folder = os.path.join("duplicates", today_str)
    report_folder = os.path.join("reports", today_str)
    os.makedirs(duplicate_folder, exist_ok=True)
    os.makedirs(report_folder, exist_ok=True)

    values_list, seen_keys, duplicates = [], set(), []

    for _, row in df.iterrows():
        # composite key
        key = (
            clean_text(row.get('foliochk')),
            clean_text(row.get('product')),
            clean_text(row.get('sch_name')),
            clean_date(row.get('rep_date'))
        )

        cleaned_row = (
            clean_text(row.get('foliochk')),
            clean_text(row.get('inv_name')),
            clean_text(row.get('address1')),
            clean_text(row.get('address2')),
            clean_text(row.get('address3')),
            clean_text(row.get('city')),
            clean_text(row.get('pincode')),
            clean_text(row.get('product')),
            clean_text(row.get('sch_name')),
            clean_date(row.get('rep_date')),
            clean_numeric(row.get('clos_bal')),
            clean_numeric(row.get('rupee_bal')),
            clean_text(row.get('jnt_name1')),
            clean_text(row.get('jnt_name2')),
            clean_text(row.get('phone_off')),
            clean_text(row.get('phone_res')),
            clean_text(row.get('email')),
            clean_text(row.get('holding_na')),
            clean_text(row.get('uin_no')),
            clean_text(row.get('pan_no')),
            clean_text(row.get('joint1_pan')),
            clean_text(row.get('joint2_pan')),
            clean_text(row.get('guard_pan')),
            clean_text(row.get('tax_status')),
            clean_text(row.get('broker_cod')),
            clean_text(row.get('subbroker')),
            clean_text(row.get('reinv_flag')),
            clean_text(row.get('bank_name')),
            clean_text(row.get('branch')),
            clean_text(row.get('ac_type')),
            clean_text(row.get('ac_no')),
            clean_text(row.get('b_address1')),
            clean_text(row.get('b_address2')),
            clean_text(row.get('b_address3')),
            clean_text(row.get('b_city')),
            clean_text(row.get('b_pincode')),
            clean_date(row.get('inv_dob')),
            clean_text(row.get('mobile_no')),
            clean_text(row.get('occupation')),
            clean_text(row.get('inv_iin')),
            clean_text(row.get('nom_name')),
            clean_text(row.get('relation')),
            clean_text(row.get('nom_addr1')),
            clean_text(row.get('nom_addr2')),
            clean_text(row.get('nom_addr3')),
            clean_text(row.get('nom_city')),
            clean_text(row.get('nom_state')),
            clean_text(row.get('nom_pincod')),
            clean_text(row.get('nom_ph_off')),
            clean_text(row.get('nom_ph_res')),
            clean_text(row.get('nom_email')),
            clean_numeric(row.get('nom_percen')),
            clean_text(row.get('nom2_name')),
            clean_text(row.get('nom2_relat')),
            clean_text(row.get('nom2_addr1')),
            clean_text(row.get('nom2_addr2')),
            clean_text(row.get('nom2_addr3')),
            clean_text(row.get('nom2_city')),
            clean_text(row.get('nom2_state')),
            clean_text(row.get('nom2_pinco')),
            clean_text(row.get('nom2_ph_of')),
            clean_text(row.get('nom2_ph_re')),
            clean_text(row.get('nom2_email')),
            clean_numeric(row.get('nom2_perce')),
            clean_text(row.get('nom3_name')),
            clean_text(row.get('nom3_relat')),
            clean_text(row.get('nom3_addr1')),
            clean_text(row.get('nom3_addr2')),
            clean_text(row.get('nom3_addr3')),
            clean_text(row.get('nom3_city')),
            clean_text(row.get('nom3_state')),
            clean_text(row.get('nom3_pinco')),
            clean_text(row.get('nom3_ph_of')),
            clean_text(row.get('nom3_ph_re')),
            clean_text(row.get('nom3_email')),
            clean_numeric(row.get('nom3_perce')),
            clean_text(row.get('ifsc_code')),
            clean_text(row.get('dp_id')),
            clean_text(row.get('demat')),
            clean_text(row.get('guard_name')),
            clean_text(row.get('brokcode')),
            clean_date(row.get('folio_date')),
            clean_text(row.get('aadhaar')),
            clean_text(row.get('tpa_linked')),
            clean_text(row.get('fh_ckyc_no')),
            clean_text(row.get('jh1_ckyc')),
            clean_text(row.get('jh2_ckyc')),
            clean_text(row.get('g_ckyc_no')),
            clean_date(row.get('jh1_dob')),
            clean_date(row.get('jh2_dob')),
            clean_date(row.get('guardian_d')),
            clean_text(row.get('amc_code')),
            clean_text(row.get('gst_state_')),
            clean_text(row.get('folio_old')),
            clean_text(row.get('scheme_fol')),
            clean_text(row.get('country')),
            source_file,
            datetime.now(),
            datetime.now()
        )

        if key in seen_keys:
            duplicates.append(row)
        else:
            seen_keys.add(key)
            values_list.append(cleaned_row)

    logger.info(f"Original rows: {len(df)}, unique after deduplication: {len(values_list)}, duplicates: {len(duplicates)}")

    if duplicates:
        dup_file = os.path.join(duplicate_folder, f"{os.path.basename(source_file)}_duplicates.csv")
        pd.DataFrame(duplicates).to_csv(dup_file, index=False)

    if not values_list:
        logger.info("No data to insert.")
        return 0  # Return 0 instead of nothing

    # Enhanced update logic - update more fields when data changes
    insert_query = f"""
    INSERT INTO {table_name} (
        foliochk, inv_name, address1, address2, address3, city, pincode, product, sch_name, rep_date,
        clos_bal, rupee_bal, jnt_name1, jnt_name2, phone_off, phone_res, email, holding_na, uin_no, pan_no,
        joint1_pan, joint2_pan, guard_pan, tax_status, broker_cod, subbroker, reinv_flag, bank_name, branch,
        ac_type, ac_no, b_address1, b_address2, b_address3, b_city, b_pincode, inv_dob, mobile_no, occupation,
        inv_iin, nom_name, relation, nom_addr1, nom_addr2, nom_addr3, nom_city, nom_state, nom_pincod, nom_ph_off,
        nom_ph_res, nom_email, nom_percen, nom2_name, nom2_relat, nom2_addr1, nom2_addr2, nom2_addr3, nom2_city,
        nom2_state, nom2_pinco, nom2_ph_of, nom2_ph_re, nom2_email, nom2_perce, nom3_name, nom3_relat, nom3_addr1,
        nom3_addr2, nom3_addr3, nom3_city, nom3_state, nom3_pinco, nom3_ph_of, nom3_ph_re, nom3_email, nom3_perce,
        ifsc_code, dp_id, demat, guard_name, brokcode, folio_date, aadhaar, tpa_linked, fh_ckyc_no, jh1_ckyc,
        jh2_ckyc, g_ckyc_no, jh1_dob, jh2_dob, guardian_d, amc_code, gst_state_, folio_old, scheme_fol, country,
        file_path, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (foliochk, product, sch_name, rep_date) DO UPDATE SET
        inv_name=EXCLUDED.inv_name, 
        address1=EXCLUDED.address1, 
        address2=EXCLUDED.address2,
        address3=EXCLUDED.address3, 
        city=EXCLUDED.city, 
        pincode=EXCLUDED.pincode,
        clos_bal=EXCLUDED.clos_bal,
        rupee_bal=EXCLUDED.rupee_bal,
        jnt_name1=EXCLUDED.jnt_name1,
        jnt_name2=EXCLUDED.jnt_name2,
        phone_off=EXCLUDED.phone_off,
        phone_res=EXCLUDED.phone_res,
        email=EXCLUDED.email,
        mobile_no=EXCLUDED.mobile_no,
        uin_no=EXCLUDED.uin_no,
        pan_no=EXCLUDED.pan_no,
        bank_name=EXCLUDED.bank_name,
        branch=EXCLUDED.branch,
        ac_type=EXCLUDED.ac_type,
        ac_no=EXCLUDED.ac_no,
        ifsc_code=EXCLUDED.ifsc_code,
        aadhaar=EXCLUDED.aadhaar,
        updated_at=EXCLUDED.updated_at,
        file_path=EXCLUDED.file_path
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_processed = 0
        for batch in [values_list[i:i+BATCH_SIZE] for i in range(0, len(values_list), BATCH_SIZE)]:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_processed += len(batch)
            logger.info(f"Inserted/updated {len(batch)} rows")

        report_file = os.path.join(report_folder, f"{os.path.basename(source_file)}_inserted_or_updated.csv")
        pd.DataFrame(values_list).to_csv(report_file, index=False)

        summary_file = os.path.join(report_folder, "summary.csv")
        with open(summary_file, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "inserted_or_updated", "in_file_duplicates"])
            writer.writerow([today_str, source_file, total_processed, len(duplicates)])

        # RETURN THE TOTAL PROCESSED COUNT
        return total_processed

    except Exception as e:
        logger.exception(f"Insert/update failed: {e}")
        conn.rollback()
        return 0  # Return 0 on error
    finally:
        cursor.close()
        conn.close()