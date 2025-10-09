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

def check_existing_records(cursor, unique_values):
    """Check which records already exist in the database"""
    query = """
    SELECT brok_dlr_c, folio, scheme_nam, asset_date 
    FROM cam_aum_wbr22 
    WHERE (brok_dlr_c, folio, scheme_nam, asset_date) IN %s
    """
    cursor.execute(query, (tuple(unique_values),))
    return {(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()}

def process_aum_dataframe(df, source_file="manual_upload.xlsx", table_name="cam_aum_wbr22"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")
    df.columns = [to_snake_case(col) for col in df.columns]
    today_str = datetime.now().strftime('%Y-%m-%d')

    report_folder = os.path.join("reports", today_str)
    os.makedirs(report_folder, exist_ok=True)

    values_list = []
    unique_keys = []

    for _, row in df.iterrows():
        brok_dlr_c = clean_text(row.get('brok_dlr_c'))
        folio = clean_text(row.get('folio'))
        scheme_nam = clean_text(row.get('scheme_nam'))
        asset_date = clean_date(row.get('asset_date'))
        
        cleaned_row = (
            brok_dlr_c,
            clean_text(row.get('product')),
            asset_date,
            folio,
            clean_text(row.get('inv_name')),
            scheme_nam,
            clean_numeric(row.get('closing_as')),
            clean_text(row.get('city')),
            clean_text(row.get('ae_code')),
            clean_text(row.get('tax_status')),
            clean_numeric(row.get('units')),
            clean_numeric(row.get('nav')),
            clean_text(row.get('inv_iin')),
            clean_text(row.get('folio_old')),
            clean_text(row.get('scheme_fol')),
            source_file,
            datetime.now(),
            datetime.now()
        )
        values_list.append(cleaned_row)
        
       
        if all([brok_dlr_c, folio, scheme_nam, asset_date]):
            unique_keys.append((brok_dlr_c, folio, scheme_nam, asset_date))

    logger.info(f"Total rows to process: {len(values_list)}")

    if not values_list:
        logger.info("No valid data to process.")
        return 0  

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        
        existing_records = check_existing_records(cursor, unique_keys) if unique_keys else set()
        logger.info(f"Found {len(existing_records)} existing records in database")
        
        insert_values = []
        update_values = []
        
       
        for i, (values, unique_key) in enumerate(zip(values_list, unique_keys)):
            if unique_key in existing_records:
                update_values.append((i, values))
            else:
                insert_values.append(values)
        
        total_inserted = 0
        total_updated = 0
        failed_operations = 0
        
      
        if insert_values:
            insert_query = f"""
            INSERT INTO {table_name} (
                brok_dlr_c, product, asset_date, folio, inv_name, scheme_nam, closing_as, city,
                ae_code, tax_status, units, nav, inv_iin, folio_old, scheme_fol,
                file_path, created_at, updated_at
            ) VALUES %s
            """
            
            for i in range(0, len(insert_values), BATCH_SIZE):
                batch = insert_values[i:i+BATCH_SIZE]
                try:
                    execute_values(cursor, insert_query, batch)
                    conn.commit()
                    total_inserted += len(batch)
                    logger.info(f"Inserted {len(batch)} new rows")
                except Exception as batch_error:
                    logger.warning(f"Insert batch failed, inserting individually: {batch_error}")
                    conn.rollback()
                    
                    for row in batch:
                        try:
                            execute_values(cursor, insert_query, [row])
                            conn.commit()
                            total_inserted += 1
                        except Exception as row_error:
                            logger.warning(f"Failed to insert row: {row_error}")
                            failed_operations += 1
                            conn.rollback()
        
        
        if update_values:
            update_query = f"""
            UPDATE {table_name} 
            SET product = %s, 
                inv_name = %s, 
                closing_as = %s, 
                city = %s, 
                ae_code = %s, 
                tax_status = %s, 
                units = %s, 
                nav = %s, 
                inv_iin = %s, 
                folio_old = %s, 
                scheme_fol = %s,
                file_path = %s,
                updated_at = %s
            WHERE brok_dlr_c = %s 
                AND folio = %s 
                AND scheme_nam = %s 
                AND asset_date = %s
            """
            
            for idx, values in update_values:
                update_data = (
                    values[1],  
                    values[4],  
                    values[6],  
                    values[7], 
                    values[8],  
                    values[9],  
                    values[10],
                    values[11],
                    values[12],
                    values[13], 
                    values[14], 
                    values[15],
                    datetime.now(), 
                    values[0],  
                    values[3],  
                    values[5],  
                    values[2]  
                )
                
                try:
                    cursor.execute(update_query, update_data)
                    conn.commit()
                    total_updated += 1
                except Exception as update_error:
                    logger.warning(f"Failed to update row {idx}: {update_error}")
                    failed_operations += 1
                    conn.rollback()
        
      
        report_path = os.path.join(report_folder, f"{os.path.basename(source_file)}_processed.csv")
        pd.DataFrame(values_list).to_csv(report_path, index=False)

        summary_path = os.path.join(report_folder, "summary.csv")
        with open(summary_path, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["date", "source_file", "total_rows", "inserted", "updated", "failed"])
            writer.writerow([today_str, source_file, len(values_list), total_inserted, total_updated, failed_operations])

        logger.info(f"Processed: Total={len(values_list)}, Inserted={total_inserted}, Updated={total_updated}, Failed={failed_operations}")
        
        return total_inserted + total_updated

    except Exception as e:
        logger.exception(f"Processing failed: {e}")
        conn.rollback()
        return 0 
    finally:
        cursor.close()
        conn.close()