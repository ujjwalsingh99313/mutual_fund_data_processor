import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime
import os
from config.config import EMAIL_USER, EMAIL_PASS, SMTP_SERVER, SMTP_PORT, NOTIFICATION_RECIPIENTS
from utils.logger import logger

class EmailNotifier:
    def __init__(self):
        self.sender = EMAIL_USER
        self.recipients = NOTIFICATION_RECIPIENTS or [EMAIL_USER]
    
    def send_email(self, subject, body, attachments=None):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = ', '.join(self.recipients)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            if attachments:
                for attachment in attachments:
                    with open(attachment, "rb") as f:
                        part = MIMEApplication(f.read(), Name=os.path.basename(attachment))
                    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
                    msg.attach(part)
            
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.send_message(msg)
            
            logger.info(f"Notification email sent: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def send_processing_success(self, file_type, filename, records_processed, table_name, mode="auto"):
        subject = f" SUCCESS: {file_type} File Processed - {filename}"
        body = f"""
        <h3>Data Processing Successful</h3>
        <p><strong>File:</strong> {filename}</p>
        <p><strong>Type:</strong> {file_type}</p>
        <p><strong>Mode:</strong> {mode.upper()}</p>
        <p><strong>Records Processed:</strong> {records_processed}</p>
        <p><strong>Target Table:</strong> {table_name}</p>
        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Status:</strong>  Data successfully inserted into database</p>
        """
        return self.send_email(subject, body)
    
    def send_processing_failure(self, file_type, filename, error_message, mode="auto", suggestion=""):
        subject = f" FAILED: {file_type} Processing Failed - {filename}"
        body = f"""
        <h3>Data Processing Failed</h3>
        <p><strong>File:</strong> {filename}</p>
        <p><strong>Type:</strong> {file_type}</p>
        <p><strong>Mode:</strong> {mode.upper()}</p>
        <p><strong>Error:</strong> {error_message}</p>
        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Status:</strong>  Processing failed</p>
        """
        
        if suggestion:
            body += f"<p><strong>Suggested Action:</strong> {suggestion}</p>"
        
        return self.send_email(subject, body)
    
    def send_job_summary(self, success_count, failure_count, details):
        subject = f" Daily Processing Summary: {success_count} Success, {failure_count} Failed"
        body = f"""
        <h3>Daily Processing Summary</h3>
        <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d')}</p>
        <p><strong>Successful Files:</strong> {success_count}</p>
        <p><strong>Failed Files:</strong> {failure_count}</p>
        <p><strong>Total Processed:</strong> {success_count + failure_count}</p>
        <hr>
        <h4>Processing Details:</h4>
        {details}
        """
        return self.send_email(subject, body)

notifier = EmailNotifier()