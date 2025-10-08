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
    """Convert column names to snake_case"""
    name = re.sub(r'[\s]+', '_', name.strip())
    name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', name)
    return name.lower()

def clean_text(val):
    """Clean text values"""
    return str(val).strip() if pd.notna(val) and str(val).strip() else None

def clean_date(val):
    """Clean and parse date values"""
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        # Handle DD-MM-YYYY format from your data
        if isinstance(val, str) and '-' in val:
            day, month, year = val.split('-')
            if len(year) == 2:  # Handle 2-digit year
                year = '20' + year if int(year) <= 50 else '19' + year
            val = f"{year}-{month}-{day}"
        
        parsed = pd.to_datetime(val, errors='coerce')
        return parsed.date() if pd.notna(parsed) else None
    except:
        return None

def clean_numeric(val):
    """Clean numeric values"""
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

def is_file_already_processed(file_hash, table_name="cam_sip_wbr5"):
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

def mark_file_as_processed(file_name, file_hash, table_name="cam_sip_wbr5"):
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

def check_unique_constraint_exists(table_name, columns):
    """Check if a unique constraint exists on the specified columns"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT conname 
            FROM pg_constraint 
            WHERE conrelid = %s::regclass 
            AND contype = 'u'
            AND conkey::int[] = (
                SELECT array_agg(attnum) 
                FROM pg_attribute 
                WHERE attrelid = %s::regclass 
                AND attname = ANY(%s)
            )
        """, (table_name, table_name, columns))
        
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"Error checking constraint: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def process_sip_expire_dataframe(df, source_file="manual_upload", table_name="cam_sip_wbr5"):
    """
    Process SIP Expire DataFrame dynamically into table_name with duplicate file prevention.
    """
    logger.info(f"Processing data into table: {table_name} from: {source_file}")
    
    # Check if file has been processed before (for actual file paths)
    if source_file != "manual_upload" and os.path.exists(source_file):
        file_hash = get_file_hash(source_file)
        if file_hash and is_file_already_processed(file_hash, table_name):
            logger.warning(f"File {source_file} has already been processed. Skipping.")
            return {"status": "skipped", "reason": "file_already_processed"}
    
    # Convert column names to snake_case
    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Create folders for duplicates and reports
    duplicate_folder = os.path.join("duplicates", today_str)
    report_folder = os.path.join("reports", today_str)
    os.makedirs(duplicate_folder, exist_ok=True)
    os.makedirs(report_folder, exist_ok=True)

    values_list, seen_keys, duplicates = [], set(), []

    # Deduplicate based on composite key (folio_no, ref_no, to_date)
    for _, row in df.iterrows():
        key = (
            clean_text(row.get('folio_no')),
            clean_text(row.get('ref_no')), 
            clean_date(row.get('to_date'))
        )
        
        # Clean all row values according to your DDL
        cleaned_row = (
            clean_text(row.get('amc_code')), clean_text(row.get('folio_no')), clean_text(row.get('ref_no')),
            clean_text(row.get('inv_name')), clean_text(row.get('address1')), clean_text(row.get('address2')),
            clean_text(row.get('address3')), clean_text(row.get('city')), clean_text(row.get('state')),
            clean_text(row.get('pincode')), clean_text(row.get('email')), clean_text(row.get('phone_off')),
            clean_text(row.get('phone_res')), clean_date(row.get('to_date')), clean_text(row.get('trxntype')),
            clean_text(row.get('sch_name')), clean_text(row.get('to_sch_nam')), clean_numeric(row.get('amount')),
            clean_numeric(row.get('units')), clean_text(row.get('brok_dlr_c')), clean_text(row.get('uin_no')),
            clean_text(row.get('sub_broker')), clean_text(row.get('tax_status')), clean_text(row.get('inv_iin')),
            clean_text(row.get('folio_old')), clean_text(row.get('scheme_fol')), clean_text(row.get('request_re')),
            source_file, datetime.now(), datetime.now()  # file_path, created_at, updated_at
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

    # Final dedup - use composite key (folio_no, ref_no, to_date)
    unique_map = { (row[1], row[2], row[13]): row for row in values_list }  # folio_no, ref_no, to_date
    deduped_values_list = list(unique_map.values())
    logger.info(f"Ready to insert/update: {len(deduped_values_list)} rows")

    if not deduped_values_list:
        logger.info("No valid data to insert.")
        return {"status": "skipped", "reason": "no_valid_data"}

    # Check if unique constraint exists
    constraint_columns = ['folio_no', 'ref_no', 'to_date']
    has_constraint = check_unique_constraint_exists(table_name, constraint_columns)
    
    if has_constraint:
        # Use ON CONFLICT with the unique constraint
        insert_query = f"""
        INSERT INTO {table_name} (
            amc_code, folio_no, ref_no, inv_name, address1, address2, address3, city, state, pincode,
            email, phone_off, phone_res, to_date, trxntype, sch_name, to_sch_nam, amount, units,
            brok_dlr_c, uin_no, sub_broker, tax_status, inv_iin, folio_old, scheme_fol, request_re,
            file_path, created_at, updated_at
        ) VALUES %s
        ON CONFLICT (folio_no, ref_no, to_date) DO UPDATE SET
            amc_code=EXCLUDED.amc_code,
            inv_name=EXCLUDED.inv_name,
            address1=EXCLUDED.address1,
            address2=EXCLUDED.address2,
            address3=EXCLUDED.address3,
            city=EXCLUDED.city,
            state=EXCLUDED.state,
            pincode=EXCLUDED.pincode,
            email=EXCLUDED.email,
            phone_off=EXCLUDED.phone_off,
            phone_res=EXCLUDED.phone_res,
            trxntype=EXCLUDED.trxntype,
            sch_name=EXCLUDED.sch_name,
            to_sch_nam=EXCLUDED.to_sch_nam,
            amount=EXCLUDED.amount,
            units=EXCLUDED.units,
            brok_dlr_c=EXCLUDED.brok_dlr_c,
            uin_no=EXCLUDED.uin_no,
            sub_broker=EXCLUDED.sub_broker,
            tax_status=EXCLUDED.tax_status,
            inv_iin=EXCLUDED.inv_iin,
            folio_old=EXCLUDED.folio_old,
            scheme_fol=EXCLUDED.scheme_fol,
            request_re=EXCLUDED.request_re,
            file_path=EXCLUDED.file_path,
            updated_at=EXCLUDED.updated_at
        """
    else:
        # Use simple INSERT and handle duplicates manually
        logger.warning(f"No unique constraint found on {constraint_columns}. Using simple INSERT.")
        insert_query = f"""
        INSERT INTO {table_name} (
            amc_code, folio_no, ref_no, inv_name, address1, address2, address3, city, state, pincode,
            email, phone_off, phone_res, to_date, trxntype, sch_name, to_sch_nam, amount, units,
            brok_dlr_c, uin_no, sub_broker, tax_status, inv_iin, folio_old, scheme_fol, request_re,
            file_path, created_at, updated_at
        ) VALUES %s
        """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_inserted = 0
        for batch in [deduped_values_list[i:i+BATCH_SIZE] for i in range(0, len(deduped_values_list), BATCH_SIZE)]:
            if has_constraint:
                execute_values(cursor, insert_query, batch)
            else:
                # For simple INSERT, we need to handle potential duplicates
                try:
                    execute_values(cursor, insert_query, batch)
                except Exception as e:
                    if "duplicate key" in str(e):
                        logger.warning("Duplicate found, inserting rows individually...")
                        # Insert rows one by one to handle duplicates gracefully
                        for single_row in batch:
                            try:
                                single_insert_query = insert_query.replace("%s", "%s")
                                cursor.execute(single_insert_query, single_row)
                                total_inserted += 1
                            except Exception as single_error:
                                if "duplicate key" not in str(single_error):
                                    logger.warning(f"Skipped duplicate row: {single_error}")
                                continue
                    else:
                        raise e
            
            conn.commit()
            if has_constraint:
                total_inserted += len(batch)
            logger.info(f"Processed {len(batch)} rows")

        # Mark file as processed if it's a real file
        if source_file != "manual_upload" and os.path.exists(source_file):
            file_hash = get_file_hash(source_file)
            if file_hash:
                mark_file_as_processed(os.path.basename(source_file), file_hash, table_name)

        # Save processing report
        report_path = os.path.join(report_folder, f"{os.path.basename(source_file).replace('.dbf','')}_processed.csv")
        
        # Create a DataFrame with column names for the report
        report_columns = [
            'amc_code', 'folio_no', 'ref_no', 'inv_name', 'address1', 'address2', 'address3', 
            'city', 'state', 'pincode', 'email', 'phone_off', 'phone_res', 'to_date', 
            'trxntype', 'sch_name', 'to_sch_nam', 'amount', 'units', 'brok_dlr_c', 
            'uin_no', 'sub_broker', 'tax_status', 'inv_iin', 'folio_old', 'scheme_fol', 
            'request_re', 'file_path', 'created_at', 'updated_at'
        ]
        
        report_df = pd.DataFrame(deduped_values_list, columns=report_columns)
        report_df.to_csv(report_path, index=False)

        # Update summary file
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

def process_sip_expire_file(file_path, table_name="cam_sip_wbr5"):
    """
    Process SIP Expire data from file (CSV, Excel, etc.)
    """
    try:
        # Determine file type and read accordingly
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.dbf'):
            # For DBF files, you might need to use dbfread or another library
            try:
                from dbfread import DBF
                dbf = DBF(file_path)
                df = pd.DataFrame(iter(dbf))
            except ImportError:
                logger.error("dbfread library required for DBF files. Install with: pip install dbfread")
                return {"status": "error", "error": "dbfread library not installed"}
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        return process_sip_expire_dataframe(df, file_path, table_name)
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return {"status": "error", "error": str(e)}