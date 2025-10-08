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

def process_trxn_wbr2_dataframe(df, source_file="manual_upload", table_name="cam_investor_trxn_wbr2"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")
    logger.info(f"Original columns: {list(df.columns)}")
    logger.info(f"Number of columns: {len(df.columns)}")
    logger.info(f"Sample data: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")
    
    df.columns = [to_snake_case(col) for col in df.columns]
    logger.info(f"Converted columns to snake_case: {list(df.columns)}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
        """)
        db_columns = [row[0] for row in cursor.fetchall()]
        logger.info(f"Database columns for {table_name}: {db_columns}")
        
        
        data_columns = [col for col in db_columns if col != 'id']
        logger.info(f"Data columns (excluding id): {data_columns}")
        
    except Exception as e:
        logger.error(f"Failed to get database schema: {e}")
        db_columns = []
        data_columns = []
    finally:
        cursor.close()
        conn.close()
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    report_folder = os.path.join("reports", today_str)
    os.makedirs(report_folder, exist_ok=True)

    values_list = []
    
  
    column_mapping = {}
    for db_col in data_columns:
        if db_col in df.columns:
            column_mapping[db_col] = db_col
        else:
          
            for df_col in df.columns:
                if db_col.lower() in df_col.lower() or df_col.lower() in db_col.lower():
                    column_mapping[db_col] = df_col
                    break
            else:
                column_mapping[db_col] = None  
    
    logger.info(f"Column mapping: {column_mapping}")

    for _, row in df.iterrows():
        try:
          
            cleaned_row = []
            for db_col in data_columns:
                if db_col in ['file_path', 'created_at', 'updated_at']:
                   
                    continue
                    
                df_col = column_mapping.get(db_col)
                if df_col and df_col in row:
                    val = row[df_col]
                    
                    if any(keyword in db_col for keyword in ['date', 'dt', 'traddate', 'postdate', 'sys_regn_d']):
                        cleaned_val = clean_date(val)
                    elif any(keyword in db_col for keyword in ['amount', 'price', 'units', 'perc', 'comm', 'tax', 'stt', 'load', 'charg', 'amt', 'duty']):
                        cleaned_val = clean_numeric(val)
                    else:
                        cleaned_val = clean_text(val)
                    cleaned_row.append(cleaned_val)
                else:
                    cleaned_row.append(None)
            
            cleaned_row.extend([source_file, datetime.now(), datetime.now()])
            values_list.append(tuple(cleaned_row))
            
        except Exception as e:
            logger.warning(f"Failed to clean row: {e}")
            continue

    logger.info(f"Total rows to process: {len(values_list)}")

    if not values_list:
        logger.info("No valid data to process.")
        return 0

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        total_inserted = 0
        total_updated = 0
        failed_rows = 0
        
        placeholders = ', '.join(['%s'] * len(data_columns))
        columns_str = ', '.join(data_columns)
        

        update_columns = [col for col in data_columns if col not in ['file_path', 'created_at', 'updated_at']]
        set_clause = ', '.join([f"{col} = %s" for col in update_columns])
        
        for i, row in enumerate(values_list):
            try:
              
                amc_code_idx = data_columns.index('amc_code') if 'amc_code' in data_columns else -1
                folio_no_idx = data_columns.index('folio_no') if 'folio_no' in data_columns else -1
                trxnno_idx = data_columns.index('trxnno') if 'trxnno' in data_columns else -1
                traddate_idx = data_columns.index('traddate') if 'traddate' in data_columns else -1
                
                amc_code = row[amc_code_idx] if amc_code_idx >= 0 else None
                folio_no = row[folio_no_idx] if folio_no_idx >= 0 else None
                trxnno = row[trxnno_idx] if trxnno_idx >= 0 else None
                traddate = row[traddate_idx] if traddate_idx >= 0 else None
              
                check_query = """
                SELECT id FROM cam_investor_trxn_wbr2 
                WHERE amc_code = %s AND folio_no = %s AND trxnno = %s AND traddate = %s
                """
                
                cursor.execute(check_query, (amc_code, folio_no, trxnno, traddate))
                existing_record = cursor.fetchone()
                
                if existing_record:
                   
                    update_query = f"""
                    UPDATE cam_investor_trxn_wbr2 SET
                        {set_clause}
                    WHERE amc_code = %s AND folio_no = %s AND trxnno = %s AND traddate = %s
                    """
                    
                    update_data = [row[data_columns.index(col)] for col in update_columns]
                    update_params = update_data + [amc_code, folio_no, trxnno, traddate]
                    cursor.execute(update_query, update_params)
                    total_updated += 1
                    
                else:
                    insert_query = f"""
                    INSERT INTO cam_investor_trxn_wbr2 ({columns_str})
                    VALUES ({placeholders})
                    """
                    cursor.execute(insert_query, row)
                    total_inserted += 1
                
                conn.commit()
                
                if (i + 1) % BATCH_SIZE == 0:
                    logger.info(f"Processed {i + 1} rows (Inserted: {total_inserted}, Updated: {total_updated})")
                    
            except Exception as row_error:
                logger.warning(f"Failed to process row {i}: {row_error}")
                failed_rows += 1
                conn.rollback()
                continue

        report_path = os.path.join(report_folder, f"{os.path.basename(source_file).replace('.dbf','')}_processed.csv")
        pd.DataFrame(values_list, columns=data_columns).to_csv(report_path, index=False)

        summary_path = os.path.join(report_folder, "summary.csv")
        with open(summary_path, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "total_rows", "inserted_rows", "updated_rows", "failed_rows"])
            writer.writerow([today_str, source_file, len(values_list), total_inserted, total_updated, failed_rows])

        logger.info(f"Total processed: {len(values_list)} (Inserted: {total_inserted}, Updated: {total_updated}, Failed: {failed_rows})")
        return total_inserted + total_updated

    except Exception as e:
        logger.exception(f"Processing failed: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()