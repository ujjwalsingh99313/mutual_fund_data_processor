import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from config.db_config import get_db_connection
from utils.logger import logger
import os
import re
import csv
import traceback

BATCH_SIZE = 500

def to_snake_case(name):
    name = re.sub(r'[\s]+', '_', name.strip())
    name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', name)
    return name.lower()

def clean_text(val):
    if pd.isna(val) or val is None:
        return None
    return str(val).strip() if str(val).strip() else None

def clean_date(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        parsed = pd.to_datetime(val, errors='coerce')
        return parsed.date() if pd.notna(parsed) else None
    except:
        return None

def process_investor_master_dataframe(df, source_file="manual_upload", table_name="kfintech_investor_master"):
    try:
        logger.info(f"Processing data into table: {table_name} from: {source_file}")

        # Convert column names to snake_case
        df.columns = [to_snake_case(col) for col in df.columns]
        
        # Check if inv_id column exists, if not create it with None values
        if 'inv_id' not in df.columns:
            df['inv_id'] = None

        today_str = datetime.now().strftime('%Y-%m-%d')
        base_folder = os.path.join("kafintech_reports", today_str)
        os.makedirs(base_folder, exist_ok=True)

        values_list = []

        for index, row in df.iterrows():
            try:
                # Clean all row values - no unique constraints to worry about
                cleaned_row = (
                    clean_text(row.get('prcode')), clean_text(row.get('fund')), clean_text(row.get('acno')),
                    clean_text(row.get('funddesc')), clean_text(row.get('invname')), clean_text(row.get('jtname1')),
                    clean_text(row.get('jtname2')), clean_text(row.get('add1')), clean_text(row.get('add2')),
                    clean_text(row.get('add3')), clean_text(row.get('city')), clean_text(row.get('pin')),
                    clean_text(row.get('state')), clean_text(row.get('country')), clean_text(row.get('tpin')),
                    clean_date(row.get('dob')), clean_text(row.get('fname')), clean_text(row.get('mname')),
                    clean_text(row.get('rphone')), clean_text(row.get('ph_res1')), clean_text(row.get('ph_res2')),
                    clean_text(row.get('ophone')), clean_text(row.get('ph_off1')), clean_text(row.get('ph_off2')),
                    clean_text(row.get('fax')), clean_text(row.get('fax_off')), clean_text(row.get('status')),
                    clean_text(row.get('occpn')), clean_text(row.get('email')), clean_text(row.get('bnkacno')),
                    clean_text(row.get('bname')), clean_text(row.get('bnkactype')), clean_text(row.get('branch')),
                    clean_text(row.get('badd1')), clean_text(row.get('badd2')), clean_text(row.get('badd3')),
                    clean_text(row.get('bcity')), clean_text(row.get('bphone')), clean_text(row.get('bstate')),
                    clean_text(row.get('bcountry')), clean_text(row.get('inv_id')), clean_text(row.get('brokcode')),
                    clean_date(row.get('crdate')), clean_text(row.get('crtime')), clean_text(row.get('pangno')),
                    clean_text(row.get('mobile')), clean_text(row.get('divopt')), clean_text(row.get('occp_desc')),
                    clean_text(row.get('modeofhold')), clean_text(row.get('mapin')), clean_text(row.get('pan2')),
                    clean_text(row.get('pan3')), clean_text(row.get('imcategory')), clean_text(row.get('guardiann0')),
                    clean_text(row.get('nominee')), clean_text(row.get('clientid')), clean_text(row.get('dpid')),
                    clean_text(row.get('categoryd1')), clean_text(row.get('statusdesc')), clean_text(row.get('ifsc')),
                    clean_text(row.get('nominee2')), clean_text(row.get('nominee3')), clean_text(row.get('kyc1flag')),
                    clean_text(row.get('kyc2flag')), clean_text(row.get('kyc3flag')), clean_text(row.get('guardpanno')),
                    clean_date(row.get('lastupdat2')), clean_text(row.get('can')), clean_text(row.get('nomineerel')),
                    clean_text(row.get('nominee2r3')), clean_text(row.get('nominee3r4')), clean_text(row.get('nomineera5')),
                    clean_text(row.get('nominee2r6')), clean_text(row.get('nominee3r7')), clean_text(row.get('adrh1info')),
                    clean_text(row.get('adrh2info')), clean_text(row.get('adrh3nfo')), clean_text(row.get('adrginfo')),
                    clean_text(row.get('nominee_a8')), clean_text(row.get('nominee_a9')), clean_text(row.get('nominee_10')),
                    clean_text(row.get('nominee_11')), clean_text(row.get('nominee_12')), clean_text(row.get('nominee_13')),
                    clean_text(row.get('nominee_14')), clean_text(row.get('nominee_15')), clean_text(row.get('nominee216')),
                    clean_text(row.get('nominee217')), clean_text(row.get('nominee218')), clean_text(row.get('nominee219')),
                    clean_text(row.get('nominee220')), clean_text(row.get('nominee221')), clean_text(row.get('nominee222')),
                    clean_text(row.get('nominee223')), clean_text(row.get('nominee324')), clean_text(row.get('nominee325')),
                    clean_text(row.get('nominee326')), clean_text(row.get('nominee327')), clean_text(row.get('nominee328')),
                    clean_text(row.get('nominee329')), clean_text(row.get('nominee330')), clean_text(row.get('nominee331')),
                    clean_text(row.get('ckyc_no')), clean_text(row.get('jh1_ckyc')), clean_text(row.get('jh2_ckyc')),
                    clean_text(row.get('guardian32')), clean_text(row.get('joint_ho33')), clean_text(row.get('joint_ho34')),
                    clean_text(row.get('investor35')), clean_text(row.get('kycgflag')), clean_text(row.get('dmtacno')),
                    clean_text(row.get('nomopt')), clean_date(row.get('nomineedob')), clean_text(row.get('jh1mobile')),
                    clean_text(row.get('jh1email')), clean_text(row.get('jh2mobile')), clean_text(row.get('jh2email')),
                    clean_text(row.get('nomineeg36')), clean_text(row.get('emailcon37')), clean_text(row.get('emailrel38')),
                    clean_text(row.get('mobilere39')), clean_text(row.get('ubo_flag')), clean_text(row.get('npo_flag')),
                    source_file, datetime.now(), datetime.now()
                )
                values_list.append(cleaned_row)
                    
            except Exception as row_error:
                logger.error(f"Error processing row {index}: {str(row_error)}")
                continue

        logger.info(f"Total rows to process: {len(values_list)}")

        if not values_list:
            logger.info("Nothing to insert.")
            return 0

        # Simple INSERT without any ON CONFLICT clause
        insert_query = f"""
        INSERT INTO {table_name} (
            prcode, fund, acno, funddesc, invname, jtname1, jtname2, add1, add2, add3, city, pin, state, country, tpin,
            dob, fname, mname, rphone, ph_res1, ph_res2, ophone, ph_off1, ph_off2, fax, fax_off, status, occpn, email,
            bnkacno, bname, bnkactype, branch, badd1, badd2, badd3, bcity, bphone, bstate, bcountry, inv_id, brokcode,
            crdate, crtime, pangno, mobile, divopt, occp_desc, modeofhold, mapin, pan2, pan3, imcategory, guardiann0,
            nominee, clientid, dpid, categoryd1, statusdesc, ifsc, nominee2, nominee3, kyc1flag, kyc2flag, kyc3flag,
            guardpanno, lastupdat2, can, nomineerel, nominee2r3, nominee3r4, nomineera5, nominee2r6, nominee3r7,
            adrh1info, adrh2info, adrh3nfo, adrginfo, nominee_a8, nominee_a9, nominee_10, nominee_11, nominee_12,
            nominee_13, nominee_14, nominee_15, nominee216, nominee217, nominee218, nominee219, nominee220, nominee221,
            nominee222, nominee223, nominee324, nominee325, nominee326, nominee327, nominee328, nominee329, nominee330,
            nominee331, ckyc_no, jh1_ckyc, jh2_ckyc, guardian32, joint_ho33, joint_ho34, investor35, kycgflag, dmtacno,
            nomopt, nomineedob, jh1mobile, jh1email, jh2mobile, jh2email, nomineeg36, emailcon37, emailrel38,
            mobilere39, ubo_flag, npo_flag, file_path, created_at, updated_at
        ) VALUES %s
        """

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            total_processed = 0
            for i in range(0, len(values_list), BATCH_SIZE):
                batch = values_list[i:i+BATCH_SIZE]
                execute_values(cursor, insert_query, batch)
                conn.commit()
                total_processed += len(batch)
                logger.info(f"Inserted batch: {len(batch)}")

            return total_processed

        except Exception as db_error:
            logger.exception(f"Database operation failed: {db_error}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.exception(f"Error in process_investor_master_dataframe: {str(e)}")
        return 0