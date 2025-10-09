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

def process_kfintech_trxn_dataframe(df, source_file="manual_upload", table_name="kafintech_trxn_report"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")

    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    base_folder = os.path.join("kafintech_reports", today_str)
    os.makedirs(base_folder, exist_ok=True)

    values_list = []

    for _, row in df.iterrows():
        cleaned_row = (
            clean_text(row.get('fmcode')), clean_text(row.get('td_fund')), clean_text(row.get('td_scheme')),
            clean_text(row.get('td_plan')), clean_text(row.get('td_acno')), clean_text(row.get('schpln')),
            clean_text(row.get('divopt')), clean_text(row.get('funddesc')), clean_text(row.get('td_purred')),
            clean_text(row.get('td_trno')), clean_text(row.get('smcode')), clean_text(row.get('chqno')),
            clean_text(row.get('invname')), clean_text(row.get('trnmode')), clean_text(row.get('trnstat')),
            clean_text(row.get('td_branch')), clean_text(row.get('isctrno')), clean_date(row.get('td_trdt')),
            clean_date(row.get('td_prdt')), clean_numeric(row.get('td_units')), clean_numeric(row.get('td_amt')),
            clean_text(row.get('td_agent')), clean_text(row.get('td_broker')), clean_numeric(row.get('brokper')),
            clean_numeric(row.get('brokcomm')), clean_text(row.get('invid')), clean_date(row.get('crdate')),
            clean_text(row.get('crtime')), clean_text(row.get('trnsub')), clean_text(row.get('td_appno')),
            clean_text(row.get('unqno')), clean_text(row.get('trdesc')), clean_text(row.get('td_trtype')),
            clean_date(row.get('navdate')), clean_date(row.get('portdt')), clean_text(row.get('assettype')),
            clean_text(row.get('subtrtype')), clean_text(row.get('citycateg0')), clean_text(row.get('euin')),
            clean_numeric(row.get('trcharges')), clean_text(row.get('clientid')), clean_text(row.get('dpid')),
            clean_numeric(row.get('stt')), clean_text(row.get('ihno')), clean_text(row.get('branchcode')),
            clean_text(row.get('inwardnum1')), clean_text(row.get('pan1')), clean_text(row.get('pan2')),
            clean_text(row.get('pan3')), clean_numeric(row.get('tdsamount')), clean_date(row.get('chqdate')),
            clean_text(row.get('chqbank')), clean_text(row.get('trflag')), clean_numeric(row.get('load1')),
            clean_date(row.get('brok_entdt')), clean_text(row.get('nctremarks')), clean_text(row.get('prcode1')),
            clean_text(row.get('status')), clean_text(row.get('schemeisin')), clean_numeric(row.get('td_nav')),
            clean_numeric(row.get('insamount')), clean_text(row.get('rejtrnoor2')), clean_text(row.get('evalid')),
            clean_text(row.get('edeclflag')), clean_text(row.get('subarncode')), clean_text(row.get('atmcardre3')),
            clean_text(row.get('atmcardst4')), clean_text(row.get('sch1')), clean_text(row.get('pln1')),
            clean_text(row.get('td_trxnmo5')), clean_text(row.get('newunqno')), clean_date(row.get('sipregdt')),
            clean_text(row.get('sipregslno')), clean_numeric(row.get('divper')), clean_text(row.get('can')),
            clean_text(row.get('exchorgtr6')), clean_text(row.get('electrxnf7')), clean_text(row.get('cleared')),
            clean_numeric(row.get('brok_valu8')), clean_text(row.get('td_pop')), clean_text(row.get('invstate')),
            clean_numeric(row.get('stampduty')),
            source_file, datetime.now(), datetime.now() 
        )
        values_list.append(cleaned_row)

    logger.info(f"Total rows to process: {len(values_list)}")

    if not values_list:
        logger.info("No valid data to process.")
        return 0  
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
       
        unqno_values = [row[30] for row in values_list if row[30]]  
        existing_records = {}
        
        if unqno_values:
            placeholders = ','.join(['%s'] * len(unqno_values))
            check_query = f"SELECT unqno, td_trno, td_trdt, td_amt FROM {table_name} WHERE unqno IN ({placeholders})"
            cursor.execute(check_query, unqno_values)
            
            for record in cursor.fetchall():
                existing_records[record[0]] = {
                    'td_trno': record[1],
                    'td_trdt': record[2],
                    'td_amt': record[3]
                }
        
       
        new_records = []
        update_records = []
        
        for record in values_list:
            unqno = record[30]  
            if not unqno or unqno not in existing_records:
                new_records.append(record)
            else:
                
                existing = existing_records[unqno]
                current_trno = record[9]   
                current_trdt = record[17]  
                current_amt = record[20]  
                
                if (existing['td_trno'] != current_trno or 
                    existing['td_trdt'] != current_trdt or 
                    existing['td_amt'] != current_amt):
                    update_records.append(record)
        
        logger.info(f"New records: {len(new_records)}, Records to update: {len(update_records)}")
        
      
        total_inserted = 0
        if new_records:
            insert_query = f"""
            INSERT INTO {table_name} (
                fmcode, td_fund, td_scheme, td_plan, td_acno, schpln, divopt, funddesc, td_purred, td_trno, smcode, chqno,
                invname, trnmode, trnstat, td_branch, isctrno, td_trdt, td_prdt, td_units, td_amt, td_agent, td_broker,
                brokper, brokcomm, invid, crdate, crtime, trnsub, td_appno, unqno, trdesc, td_trtype, navdate, portdt,
                assettype, subtrtype, citycateg0, euin, trcharges, clientid, dpid, stt, ihno, branchcode, inwardnum1, pan1,
                pan2, pan3, tdsamount, chqdate, chqbank, trflag, load1, brok_entdt, nctremarks, prcode1, status, schemeisin,
                td_nav, insamount, rejtrnoor2, evalid, edeclflag, subarncode, atmcardre3, atmcardst4, sch1, pln1, td_trxnmo5,
                newunqno, sipregdt, sipregslno, divper, can, exchorgtr6, electrxnf7, cleared, brok_valu8, td_pop, invstate,
                stampduty, file_path, created_at, updated_at
            ) VALUES %s
            """
            
            for i in range(0, len(new_records), BATCH_SIZE):
                batch = new_records[i:i+BATCH_SIZE]
                
                try:
                    execute_values(cursor, insert_query, batch)
                    conn.commit()
                    total_inserted += len(batch)
                    logger.info(f"Inserted {len(batch)} new rows (batch {i//BATCH_SIZE + 1})")
                except Exception as batch_error:
                    logger.warning(f"Batch {i//BATCH_SIZE + 1} failed, inserting rows individually: {batch_error}")
                    conn.rollback()
                    
                    for j, row in enumerate(batch):
                        try:
                            execute_values(cursor, insert_query, [row])
                            conn.commit()
                            total_inserted += 1
                        except Exception as row_error:
                            logger.warning(f"Failed to insert row {i + j}: {row_error}")
                            conn.rollback()
                            continue
        

        total_updated = 0
        if update_records:
            update_query = f"""
            UPDATE {table_name} SET
                fmcode = %s, td_fund = %s, td_scheme = %s, td_plan = %s, td_acno = %s, schpln = %s, 
                divopt = %s, funddesc = %s, td_purred = %s, td_trno = %s, smcode = %s, chqno = %s,
                invname = %s, trnmode = %s, trnstat = %s, td_branch = %s, isctrno = %s, td_trdt = %s,
                td_prdt = %s, td_units = %s, td_amt = %s, td_agent = %s, td_broker = %s, brokper = %s,
                brokcomm = %s, invid = %s, crdate = %s, crtime = %s, trnsub = %s, td_appno = %s,
                trdesc = %s, td_trtype = %s, navdate = %s, portdt = %s, assettype = %s, subtrtype = %s,
                citycateg0 = %s, euin = %s, trcharges = %s, clientid = %s, dpid = %s, stt = %s,
                ihno = %s, branchcode = %s, inwardnum1 = %s, pan1 = %s, pan2 = %s, pan3 = %s,
                tdsamount = %s, chqdate = %s, chqbank = %s, trflag = %s, load1 = %s, brok_entdt = %s,
                nctremarks = %s, prcode1 = %s, status = %s, schemeisin = %s, td_nav = %s, insamount = %s,
                rejtrnoor2 = %s, evalid = %s, edeclflag = %s, subarncode = %s, atmcardre3 = %s,
                atmcardst4 = %s, sch1 = %s, pln1 = %s, td_trxnmo5 = %s, newunqno = %s, sipregdt = %s,
                sipregslno = %s, divper = %s, can = %s, exchorgtr6 = %s, electrxnf7 = %s, cleared = %s,
                brok_valu8 = %s, td_pop = %s, invstate = %s, stampduty = %s, file_path = %s, updated_at = %s
            WHERE unqno = %s
            """
            
            for record in update_records:
                try:
                   
                    update_data = record[:87] + (source_file, datetime.now(), record[30])  
                    cursor.execute(update_query, update_data)
                    conn.commit()
                    total_updated += 1
                except Exception as update_error:
                    logger.warning(f"Failed to update record with unqno {record[30]}: {update_error}")
                    conn.rollback()
                    continue
            
            logger.info(f"Updated {total_updated} existing records")

     
        report_path = os.path.join(base_folder, f"{os.path.basename(source_file)}_processed.csv")
        pd.DataFrame(values_list).to_csv(report_path, index=False)

        summary_path = os.path.join(base_folder, "summary.csv")
        with open(summary_path, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "total_rows", "inserted_rows", "updated_rows"])
            writer.writerow([today_str, source_file, len(values_list), total_inserted, total_updated])

        logger.info(f"Total processed: {len(values_list)}, Inserted: {total_inserted}, Updated: {total_updated}")
        
     
        return total_inserted + total_updated

    except Exception as e:
        logger.exception(f"Processing failed: {e}")
        conn.rollback()
        return 0  
    finally:
        cursor.close()
        conn.close()