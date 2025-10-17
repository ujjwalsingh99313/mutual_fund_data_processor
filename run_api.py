from fastapi import FastAPI, UploadFile, File, HTTPException, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import shutil
import os
import pandas as pd
from dbfread import DBF

from config import config
from processor.mutual_fund_processor import process_combined_excel
from processor.process_dbf import process_cam_inv_dataframe
from processor.process_cam_aum_daily import process_aum_dataframe
from processor.process_investor_details import process_investor_details_dataframe
from processor.process_investor_trxn import process_trxn_wbr2_dataframe
from processor.process_sip import process_sip_wbr49_dataframe
from processor.process_sip_expire import process_sip_expire_dataframe
from processor.process_cam_brokerage import process_cam_brokerage_dataframe
from processor.kafintech_aum import process_kfintech_aum_dataframe
from processor.kafintech_trxn_report import process_kfintech_trxn_dataframe
from processor.kafintech_investor_master import process_investor_master_dataframe
from processor.kfintech_brokerage import process_kfintech_brokerage_dataframe

from utils.notifier import notifier
from main import main

# Import SIP Alert Service from processor folder
from processor.sip_alert_service import sip_alert_service

app = FastAPI(
    title="Mutual Fund Data Processing API",
    description="""
This API allows secure uploading and processing of **Excel** and **DBF** files 
for CAMS and KFintech mutual fund data.  
It also supports triggering scheduled email jobs manually.  
Now with SIP Alert functionality for customer notifications.
""",
    version="1.1.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


API_TOKEN = os.getenv("API_TOKEN", "ujjwal@1107")

class StatusResponse(BaseModel):
    status: str
    filename: str | None = None

# New response model for SIP Alerts
class SIPAlertResponse(BaseModel):
    status: str
    message: str
    details: dict

def authorize(token: str = Header(..., description="API authentication token")):
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Unauthorized")



@app.post("/run-job", response_model=StatusResponse, tags=["Job"])
def run_job(token: str = Header(..., description="API authentication token")):
    """
    Manually trigger the **email reading and processing job**.  
    This runs the background batch job (`main()`).
    """
    authorize(token)
    main()
    return {"status": "job triggered"}


@app.post("/upload-excel", response_model=StatusResponse, tags=["Mutual Fund"])
async def upload_excel(
    file: UploadFile = File(..., description="Excel file (.xls/.xlsx/.csv)"),
    token: str = Header(..., description="API authentication token")
):
    """
    Upload an **Excel file** for mutual fund data processing.  
    The file is stored in `ATTACHMENT_FOLDER` and processed with `process_combined_excel`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        records_processed = process_combined_excel(file_path)
        notifier.send_processing_success(
            "Excel", file.filename, records_processed, "multiple_tables", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "Excel", file.filename, str(e), "manual",
            "Please check file format and try again."
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cams/wbr2c", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_inv(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs Investor DBF (wbr2c)** file.  
    Processes data into `cam_inv_wbr2c` using `process_cam_inv_dataframe`.
    """

    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_cam_inv_dataframe(df, source_file=file.filename)
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_inv_wbr2c", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/cams/wbr22", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_aum(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs AUM DBF (wbr22)** file.  
    Processes data into `cam_aum_wbr22` using `process_aum_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_aum_dataframe(df, source_file=file.filename, table_name="cam_aum_wbr22")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_aum_wbr22", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/cams/wbr9", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_investor(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs Investor Details DBF (wbr9)** file.  
    Processes data into `cam_investor_details_wbr9` using `process_investor_details_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_investor_details_dataframe(df, source_file=file.filename, table_name="cam_investor_details_wbr9")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_investor_details_wbr9", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/cams/wbr2", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_trxn(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs Transaction DBF (wbr2)** file.  
    Processes data into `cam_investor_trxn_wbr2` using `process_trxn_wbr2_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_trxn_wbr2_dataframe(df, source_file=file.filename, table_name="cam_investor_trxn_wbr2")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_investor_trxn_wbr2", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}") 


@app.post("/cams/wbr49", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_sip(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs Transaction DBF (wbr49)** file.  
    Processes data into `cam_sip_wbr49` using `process_sip_wbr49_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_sip_wbr49_dataframe(df, source_file=file.filename, table_name="cam_sip_wbr49")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_sip_wbr49", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/cams/wbr77", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_brokerage(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs Transaction DBF (wbr77)** file.  
    Processes data into `cam_brokerage_wbr77` using `process_cam_brokerage_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_cam_brokerage_dataframe(df, source_file=file.filename, table_name="cam_brokerage_wbr77")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_brokerage_wbr77", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
    


@app.post("/cams/wbr5", response_model=StatusResponse, tags=["CAMS"])
async def upload_cam_sip_expire(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **CAMs Transaction DBF (wbr5)** file.  
    Processes data into `cam_sip_wbr5` using `process_sip_expire_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_sip_expire_dataframe(df, source_file=file.filename, table_name="cam_sip_wbr5")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "cam_sip_wbr5", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/kfintech/wbcum", response_model=StatusResponse, tags=["KFintech"])
async def upload_kfin_aum(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **KFintech AUM DBF (wbcum)** file.  
    Processes data into `kfintech_client_aum` using `process_kfintech_aum_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_kfintech_aum_dataframe(df, source_file=file.filename, table_name="kfintech_client_aum")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "kfintech_client_aum", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/kfintech/wbtrn", response_model=StatusResponse, tags=["KFintech"])
async def upload_kfin_trxn(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **KFintech Transaction DBF (wbtrn)** file.  
    Processes data into `kafintech_trxn_report` using `process_kfintech_trxn_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_kfintech_trxn_dataframe(df, source_file=file.filename, table_name="kafintech_trxn_report")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "kafintech_trxn_report", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/kfintech/wbmst", response_model=StatusResponse, tags=["KFintech"])
async def upload_kfin_investor(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **KFintech Investor Master DBF (wbmst)** file.  
    Processes data into `kfintech_investor_master` using `process_investor_master_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_investor_master_dataframe(df, source_file=file.filename, table_name="kfintech_investor_master")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "kfintech_investor_master", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
    

@app.post("/kfintech/wbbrok", response_model=StatusResponse, tags=["KFintech"])
async def upload_kfintech_brokerage(file: UploadFile = File(...), token: str = Header(...)):
    """
    Upload **kfintech_brokerage DBF (wbmst)** file.  
    Processes data into `kfintech_brokerage` using `process_kfintech_brokerage_dataframe`.
    """
    authorize(token)
    os.makedirs(config.ATTACHMENT_FOLDER, exist_ok=True)
    file_path = os.path.join(config.ATTACHMENT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        dbf_table = DBF(file_path, load=True, encoding="latin1", ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf_table))
        records_processed = process_kfintech_brokerage_dataframe(df, source_file=file.filename, table_name="kfintech_brokerage")
        
        notifier.send_processing_success(
            "DBF", file.filename, records_processed, "kfintech_brokerage", "manual"
        )
        return {"status": "file processed", "filename": file.filename}
        
    except Exception as e:
        notifier.send_processing_failure(
            "DBF", file.filename, str(e), "manual",
            "Please verify the DBF file format and try again."
        )
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

# =============================
# NEW SIP ALERT ENDPOINTS (ADDED AT THE END)
# =============================

@app.post("/sip-alerts/send", response_model=SIPAlertResponse, tags=["SIP Alerts"])
def send_sip_alerts(
    background_tasks: BackgroundTasks,
    token: str = Header(..., description="API authentication token")
):
    """
    Send SIP deduction alerts to customers.  
    Alerts are sent 1 day before SIP deduction date.
    Includes test SMS to 9931399129.
    """
    authorize(token)
    
    try:
        result = sip_alert_service.send_sip_alerts()
        
        if 'error' in result:
            return SIPAlertResponse(
                status="error",
                message="Failed to send SIP alerts",
                details=result
            )
        
        return SIPAlertResponse(
            status="success",
            message=f"SIP alerts sent successfully",
            details=result
        )
        
    except Exception as e:
        return SIPAlertResponse(
            status="error",
            message="Error sending SIP alerts",
            details={"error": str(e)}
        )

@app.get("/sip-alerts/upcoming", response_model=SIPAlertResponse, tags=["SIP Alerts"])
def get_upcoming_sip_alerts(token: str = Header(..., description="API authentication token")):
    """
    Get list of investors with upcoming SIP deductions.
    """
    authorize(token)
    
    try:
        investors = sip_alert_service.get_investors_with_upcoming_sip()
        
        return SIPAlertResponse(
            status="success",
            message=f"Found {len(investors)} investors with upcoming SIP",
            details={
                "investors": investors,
                "count": len(investors),
                "alert_days_before": 1
            }
        )
        
    except Exception as e:
        return SIPAlertResponse(
            status="error",
            message="Error fetching upcoming SIP alerts",
            details={"error": str(e)}
        )

@app.post("/sip-alerts/send-background", response_model=StatusResponse, tags=["SIP Alerts"])
def send_sip_alerts_background(
    background_tasks: BackgroundTasks,
    token: str = Header(..., description="API authentication token")
):
    """
    Send SIP alerts in background task (non-blocking).
    """
    authorize(token)
    
    background_tasks.add_task(sip_alert_service.send_sip_alerts)
    
    return StatusResponse(
        status="background_task_started",
        filename="sip_alerts"
    )

@app.post("/sip-alerts/test", response_model=SIPAlertResponse, tags=["SIP Alerts"])
def test_sip_alert(
    token: str = Header(..., description="API authentication token")
):
    """
    Test SIP alert with your mobile number 9931399129
    """
    authorize(token)
    
    try:
        from processor.sms_service import sms_service
        
        # Test with your specific mobile number
        test_result = sms_service.send_sip_alert(
            mobile="9931399129",
            investor_name="Test Investor",
            scheme_name="ABSL Flexi Cap Fund",
            sip_amount=5000.00,
            sip_date="25/09/2025"
        )
        
        return SIPAlertResponse(
            status="success" if test_result else "error",
            message="Test SMS sent successfully" if test_result else "Test SMS failed",
            details={
                "test_mobile": "9931399129",
                "success": test_result
            }
        )
        
    except Exception as e:
        return SIPAlertResponse(
            status="error",
            message="Error sending test SMS",
            details={"error": str(e)}
        )