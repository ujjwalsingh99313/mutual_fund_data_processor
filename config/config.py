import os
from dotenv import load_dotenv
load_dotenv()

# Email Configuration
EMAIL_USER = os.getenv("EMAIL_ADDRESS")  
EMAIL_PASS = os.getenv("EMAIL_PASSWORD")  
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", 993))

# Database Configuration
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD") 
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# File System Configuration
ATTACHMENT_FOLDER = os.getenv("ATTACHMENT_DIR", "attachments")  
ARCHIVE_FOLDER = os.getenv("ARCHIVE_DIR", "archive")
UPLOAD_FOLDER = os.getenv("UPLOAD_DIR", "uploads")
LOG_FILE = os.getenv("LOG_FILE", "logs/job.log")

# API Configuration
API_TOKEN = os.getenv("API_TOKEN", "ujjwal@1107")
API_PORT = int(os.getenv("API_PORT", 8000))

# Email Notification Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
NOTIFICATION_RECIPIENTS = os.getenv("NOTIFICATION_RECIPIENTS", "").split(",")

# Nimbus SMS Configuration
NIMBUS_UID = os.getenv("NIMBUS_UID")
NIMBUS_PWD = os.getenv("NIMBUS_PWD")
NIMBUS_SENDERID = os.getenv("NIMBUS_SENDERID")
NIMBUS_ENTITYID = os.getenv("NIMBUS_ENTITYID")
NIMBUS_TEMPLATEID = os.getenv("NIMBUS_TEMPLATEID")
NIMBUS_URL = os.getenv("NIMBUS_URL")
SIP_ALERT_DAYS_BEFORE = int(os.getenv("SIP_ALERT_DAYS_BEFORE", 1))

# Test Mode
SIP_ALERT_TEST_MODE = os.getenv("SIP_ALERT_TEST_MODE", "True").lower() == "true"
SIP_ALERT_TEST_NUMBER = os.getenv("SIP_ALERT_TEST_NUMBER", "9931399129")
