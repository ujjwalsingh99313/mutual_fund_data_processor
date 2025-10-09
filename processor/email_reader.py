import imaplib
import email
from email.header import decode_header
import os
from datetime import datetime
from config.config import IMAP_SERVER, EMAIL_USER, EMAIL_PASS, ATTACHMENT_FOLDER, IMAP_PORT
from utils.logger import logger


def download_today_attachments():
    today = datetime.today().strftime('%d-%b-%Y')
    attachments = []

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        status, messages = mail.search(None, f'(UNSEEN ON "{today}")')
        if status != "OK":
            logger.warning("No emails found today.")
            return attachments

        email_ids = messages[0].split()
        for eid in email_ids:
            res, msg_data = mail.fetch(eid, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding or "utf-8", errors="ignore")

                    logger.info(f"Processing email: {subject}")

                    for part in msg.walk():
                        if part.get_content_maintype() == "multipart":
                            continue
                        if part.get("Content-Disposition") is None:
                            continue

                        filename = part.get_filename()
                        if filename:
                            filename, encoding = decode_header(filename)[0]
                            if isinstance(filename, bytes):
                                filename = filename.decode(encoding or "utf-8", errors="ignore")

                            #  Only allow Excel/CSV files
                            if not filename.lower().endswith(('.xlsx', '.xls', '.csv')):
                                logger.info(f"Skipping non-excel file: {filename}")
                                continue

                            os.makedirs(ATTACHMENT_FOLDER, exist_ok=True)
                            filepath = os.path.join(ATTACHMENT_FOLDER, filename)

                            with open(filepath, "wb") as f:
                                f.write(part.get_payload(decode=True))

                            logger.info(f"Saved attachment: {filepath}")
                            attachments.append(filepath)

        mail.logout()

    except Exception as e:
        logger.error(f"Email read error: {e}")
    return attachments
