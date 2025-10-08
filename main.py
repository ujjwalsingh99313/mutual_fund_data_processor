from utils.logger import logger
from config.config import ATTACHMENT_FOLDER
from processor.email_reader import download_today_attachments
from processor.mutual_fund_processor import process_combined_excel as process_excel_file
from processor.email_dbf_download_handler import process_dbf_download_links
from processor.process_dbf import process_cam_inv_dataframe
from processor.process_sip import process_sip_wbr49_dataframe
from processor.process_sip_expire import process_sip_expire_dataframe
from processor.process_kyc_status import process_kyc_status_dataframe
from processor.process_investor_details import process_investor_details_dataframe
from processor.process_investor_trxn import process_trxn_wbr2_dataframe
from processor.process_cam_aum_daily import process_aum_dataframe
from processor.process_cam_brokerage import process_cam_brokerage_dataframe
from processor.kafintech_aum import process_kfintech_aum_dataframe
from processor.kafintech_trxn_report import process_kfintech_trxn_dataframe
from processor.kafintech_investor_master import process_investor_master_dataframe
from processor.kfintech_brokerage import process_kfintech_brokerage_dataframe
from utils.notifier import notifier
# Added import for SIP alert service
from processor.sip_alert_service import sip_alert_service
import os
import pandas as pd


SUBJECT_TABLE_MAP = {
     "wbr22": "cam_aum_wbr22",
     "wbr2c": "cam_inv_wbr2c",
     "wbr49": "cam_sip_wbr49",
     "wbr5" : "cam_sip_wbr5" ,
     "wbr9c": "cam_investor_kyc_status_wbr9c",
     "wbr9": "cam_investor_details_wbr9",
     "wbr2": "cam_investor_trxn_wbr2",
     "wbr77": "cam_brokerage_wbr77",
     "wbcum": "kfintech_client_aum" ,
     "wbtrn": "kafintech_trxn_report" ,
     "wbmst" :"kfintech_investor_master",
     "wbbrok" : "kfintech_brokerage"
}

def get_table_name_from_subject(subject):
    subject_lower = subject.lower()
    logger.info(f"Checking subject: {subject_lower}")
    for code, table in sorted(SUBJECT_TABLE_MAP.items(), key=lambda x: -len(x[0])):
        if code.lower() in subject_lower:
            logger.info(f"Detected code '{code}' in subject -> Table: {table}")
            return table
    logger.warning(f"No matching code found in subject: {subject}")
    return None

def main():
    logger.info(" Starting full email job...")
    processing_summary = {
        'success': 0,
        'failures': 0,
        'details': []
    }
    
    try:
        os.makedirs(ATTACHMENT_FOLDER, exist_ok=True)

        # Process Excel attachments
        attachments = download_today_attachments()
        if attachments:
            logger.info(f" Found {len(attachments)} Excel attachments")
            for file_path in attachments:
                try:
                    logger.info(f" Processing Excel: {file_path}")
                    records_processed = process_excel_file(file_path)
                    filename = os.path.basename(file_path)
                    notifier.send_processing_success(
                        "Excel", filename, records_processed, "multiple_tables", "auto"
                    )
                    processing_summary['success'] += 1
                    processing_summary['details'].append(f" Excel: {filename} - {records_processed} records")
                    
                except Exception as e:
                    logger.error(f" Error processing Excel {file_path}: {e}")
                    filename = os.path.basename(file_path)
                    notifier.send_processing_failure(
                        "Excel", filename, str(e), "auto",
                        "Please check the file format and try manual upload if needed."
                    )
                    processing_summary['failures'] += 1
                    processing_summary['details'].append(f" Excel: {filename} - Error: {str(e)}")
        else:
            logger.info(" No Excel attachments found today.")

        # Process DBF / CSV attachments
        dbf_data = process_dbf_download_links()
        if dbf_data:
            logger.info(f" Found {len(dbf_data)} DBF attachments")
            for dbf_info in dbf_data:
                try:
                    filename = dbf_info.get("file")
                    rows = dbf_info.get("rows")
                    subject = dbf_info.get("subject")
                    df = pd.DataFrame(rows)
                    logger.info(f" Found DBF: {filename} with {len(df)} rows (Subject: {subject})")

                    table_name = get_table_name_from_subject(subject)
                    if not table_name:
                        logger.warning(f" Could not detect table from subject: {subject}. Skipping.")
                        continue

                    logger.info(f" Processing into table: {table_name}")

                    # Dispatch based on table_name
                    records_processed = 0
                    if table_name == "cam_sip_wbr49":
                        records_processed = process_sip_wbr49_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "cam_sip_wbr5":
                        records_processed = process_sip_expire_dataframe(df, table_name=table_name, source_file=filename)    
                    elif table_name == "cam_investor_kyc_status_wbr9c":
                        records_processed = process_kyc_status_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "cam_investor_details_wbr9":
                        records_processed = process_investor_details_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "cam_investor_trxn_wbr2":
                        records_processed = process_trxn_wbr2_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "cam_aum_wbr22":
                        records_processed = process_aum_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "cam_brokerage_wbr77":
                        records_processed = process_cam_brokerage_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "kfintech_client_aum":
                        records_processed = process_kfintech_aum_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "kafintech_trxn_report":
                        records_processed = process_kfintech_trxn_dataframe(df, table_name=table_name, source_file=filename)
                    elif table_name == "kfintech_investor_master":
                        records_processed = process_investor_master_dataframe(df, table_name=table_name, source_file=filename) 
                    elif table_name == "kfintech_brokerage":
                        records_processed = process_kfintech_brokerage_dataframe(df, table_name=table_name, source_file=filename)        
                    else:
                        records_processed = process_cam_inv_dataframe(df, table_name=table_name, source_file=filename)
                    
                    notifier.send_processing_success(
                        "DBF", filename, records_processed, table_name, "auto"
                    )
                    processing_summary['success'] += 1
                    processing_summary['details'].append(f" DBF: {filename} - {records_processed} records -> {table_name}")
                    
                except Exception as e:
                    logger.error(f" Error processing DBF {filename}: {e}")
                    notifier.send_processing_failure(
                        "DBF", filename, str(e), "auto",
                        "Please try manual upload through API endpoint."
                    )
                    processing_summary['failures'] += 1
                    processing_summary['details'].append(f" DBF: {filename} - Error: {str(e)}")
        else:
            logger.info(" No DBF links found in email body.")

        # Send summary email
        if processing_summary['details']:
            details_html = "<ul>" + "".join([f"<li>{detail}</li>" for detail in processing_summary['details']]) + "</ul>"
            notifier.send_job_summary(
                processing_summary['success'],
                processing_summary['failures'],
                details_html
            )

        # Trigger automatic SIP alerts after sending job summary
        try:
            logger.info("Triggering automatic SIP alerts as part of job run...")
            sip_result = sip_alert_service.send_sip_alerts()
            logger.info("SIP alert run result: %s", sip_result)
        except Exception as e:
            logger.exception("Automatic SIP alert run failed: %s", e)

        logger.info(" Job completed successfully.")

    except Exception as e:
        logger.exception(f" Unexpected error in main job: {e}")
        notifier.send_processing_failure(
            "JOB", "Full Automation Job", str(e), "auto",
            "Check server logs and restart the job manually."
        )

if __name__ == "__main__":
    main()
