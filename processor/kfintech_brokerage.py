import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from config.db_config import get_db_connection
from utils.logger import logger
import os
import re
import csv

BATCH_SIZE = 1000  # Increased batch size

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

def clean_numeric(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        # Remove commas and any non-numeric characters except decimal point
        cleaned_val = re.sub(r'[^\d.-]', '', str(val))
        return float(cleaned_val) if cleaned_val else None
    except:
        return None

def process_kfintech_brokerage_dataframe(df, source_file="manual_upload", table_name="kfintech_brokerage"):
    logger.info(f"Processing data into table: {table_name} from: {source_file}")
    logger.info(f"Original dataframe shape: {df.shape}")
    
    # Convert column names to snake_case
    df.columns = [to_snake_case(col) for col in df.columns]
    logger.info(f"Converted columns to snake_case: {list(df.columns)}")
    
    # Get database schema
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
        
        # Exclude id from data columns
        data_columns = [col for col in db_columns if col != 'id']
        logger.info(f"Data columns (excluding id): {data_columns}")
        
    except Exception as e:
        logger.error(f"Failed to get database schema: {e}")
        db_columns = []
        data_columns = []
    finally:
        cursor.close()
        conn.close()
    
    # Create report folder
    today_str = datetime.now().strftime('%Y-%m-%d')
    report_folder = os.path.join("reports", today_str)
    os.makedirs(report_folder, exist_ok=True)

    # Batch processing for better performance
    total_rows = len(df)
    successful_rows = 0
    failed_rows = 0
    
    # Process data in batches
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        batch_df = df.iloc[batch_start:batch_end]
        
        logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1}: rows {batch_start} to {batch_end-1}")
        
        batch_success = process_batch(
            batch_df, data_columns, source_file, table_name, 
            batch_start, total_rows
        )
        
        successful_rows += batch_success
        failed_rows += (len(batch_df) - batch_success)
    
    # Generate summary report
    summary_path = os.path.join(report_folder, "kfintech_summary.csv")
    with open(summary_path, "a", newline='') as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(["date", "source_file", "total_rows", "successful_rows", "failed_rows"])
        writer.writerow([today_str, source_file, total_rows, successful_rows, failed_rows])

    logger.info(f"Processing Complete: Total rows: {total_rows}, Successful: {successful_rows}, Failed: {failed_rows}")
    return successful_rows

def process_batch(batch_df, data_columns, source_file, table_name, batch_start, total_rows):
    """Process a batch of rows efficiently"""
    batch_values = []
    
    # Create column mapping for this batch
    column_mapping = {}
    for db_col in data_columns:
        if db_col in batch_df.columns:
            column_mapping[db_col] = db_col
        else:
            for df_col in batch_df.columns:
                if db_col.lower() in df_col.lower() or df_col.lower() in db_col.lower():
                    column_mapping[db_col] = df_col
                    break
            else:
                column_mapping[db_col] = None
    
    # Process all rows in the batch
    for _, row in batch_df.iterrows():
        try:
            cleaned_row = []
            for db_col in data_columns:
                if db_col in ['file_path', 'created_at', 'updated_at']:
                    continue
                    
                df_col = column_mapping.get(db_col)
                if df_col and df_col in row:
                    val = row[df_col]
                    
                    # Apply appropriate cleaning
                    if any(keyword in db_col for keyword in ['date', 'dt', 'fromdate', 'todate', 'processda3', 'valuedate', 'navdate', 'sipregdate', 'paymentdt', 'clbfromdt', 'wardate']):
                        cleaned_val = clean_date(val)
                    elif any(keyword in db_col for keyword in ['amount', 'units', 'percentage', 'brokerage', 'cnav', 'avgassets', 'grossbrok5', 'stxamt', 'educessamt', 'redunits', 'redamt', 'clbslabm11', 'purbrokt12', 'purnetamt', 'purgross13', 'cgstrate', 'cgstamt', 'sgstrate', 'sgstamt', 'igstrate', 'igstamt', 'ugstrate', 'ugstamt', 'totgstrate', 'totgstamt', 'amcschgs16']):
                        cleaned_val = clean_numeric(val)
                    else:
                        cleaned_val = clean_text(val)
                    cleaned_row.append(cleaned_val)
                else:
                    cleaned_row.append(None)
            
            # Add file_path and timestamps
            cleaned_row.extend([source_file, datetime.now(), datetime.now()])
            batch_values.append(tuple(cleaned_row))
            
        except Exception as e:
            logger.warning(f"Failed to clean row in batch: {e}")
            continue
    
    if not batch_values:
        return 0
    
    # Bulk insert/update using execute_values for better performance
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Prepare columns
        columns_str = ', '.join(data_columns)
        placeholders = ', '.join(['%s'] * len(data_columns))
        
        # Use ON CONFLICT for bulk upsert (PostgreSQL 9.5+)
        conflict_columns = ['prcode', 'accountno', 'trno', 'fromdate']
        
        # Check which conflict columns exist in our data
        available_conflict_cols = [col for col in conflict_columns if col in data_columns]
        
        if len(available_conflict_cols) >= 2:  # At least 2 columns for unique constraint
            update_columns = [col for col in data_columns if col not in ['file_path', 'created_at', 'updated_at'] + available_conflict_cols]
            set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            conflict_clause = f"({', '.join(available_conflict_cols)})"
            
            insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES %s
            ON CONFLICT {conflict_clause} 
            DO UPDATE SET 
                {set_clause},
                updated_at = EXCLUDED.updated_at
            """
            
            execute_values(cursor, insert_query, batch_values)
            conn.commit()
            
            successful_count = len(batch_values)
            logger.info(f"Batch {batch_start//BATCH_SIZE + 1} processed: {successful_count} rows")
            
        else:
            # Fallback to simple insert if conflict columns not available
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
            execute_values(cursor, insert_query, batch_values)
            conn.commit()
            
            successful_count = len(batch_values)
            logger.info(f"Batch {batch_start//BATCH_SIZE + 1} inserted: {successful_count} rows")
        
        return successful_count
        
    except Exception as e:
        logger.error(f"Failed to process batch {batch_start//BATCH_SIZE + 1}: {e}")
        conn.rollback()
        
        # Fallback: Try individual inserts for the failed batch
        return fallback_individual_inserts(cursor, conn, batch_values, data_columns, table_name)
    
    finally:
        cursor.close()
        conn.close()

def fallback_individual_inserts(cursor, conn, batch_values, data_columns, table_name):
    """Fallback method for individual inserts if bulk operation fails"""
    successful_count = 0
    columns_str = ', '.join(data_columns)
    placeholders = ', '.join(['%s'] * len(data_columns))
    
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    for i, row in enumerate(batch_values):
        try:
            cursor.execute(insert_query, row)
            successful_count += 1
        except Exception as e:
            logger.warning(f"Failed individual insert for row {i}: {e}")
            continue
    
    conn.commit()
    return successful_count

def process_kfintech_brokerage_file(file_path, table_name="kfintech_brokerage"):
    """
    Main function to process KFinTech brokerage files
    Supports CSV, Excel, and other formats with performance optimizations
    """
    try:
        # Determine file type and read accordingly with optimizations
        if file_path.lower().endswith('.csv'):
            # Use faster CSV reading with specific data types
            df = pd.read_csv(file_path, low_memory=False)
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            logger.error(f"Unsupported file format: {file_path}")
            return 0
        
        logger.info(f"Successfully loaded file: {file_path} with {len(df)} rows")
        
        # Optimize dataframe memory usage
        df = optimize_dataframe(df)
        
        return process_kfintech_brokerage_dataframe(df, file_path, table_name)
        
    except Exception as e:
        logger.exception(f"Failed to process KFinTech brokerage file {file_path}: {e}")
        return 0

def optimize_dataframe(df):
    """Optimize dataframe for better memory usage and performance"""
    try:
        # Convert object columns to category where appropriate
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:  # If unique values < 50% of total
                df[col] = df[col].astype('category')
        
        # Convert float64 to float32 where precision allows
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')
            
    except Exception as e:
        logger.warning(f"Dataframe optimization failed: {e}")
    
    return df