import imaplib
import email
from email.header import decode_header
import os
from datetime import datetime, timedelta
import re
import requests
import pyzipper
from dbfread import DBF
import pandas as pd
from bs4 import BeautifulSoup
from config.config import IMAP_SERVER, EMAIL_USER, EMAIL_PASS, ATTACHMENT_FOLDER, IMAP_PORT
from utils.logger import logger
import json



PROCESSED_LOG = os.path.join(ATTACHMENT_FOLDER, "processed_files.json")

def load_processed_files():
    """Load processed file keys from JSON log file."""
    if os.path.exists(PROCESSED_LOG):
        try:
            with open(PROCESSED_LOG, "r") as f:
                return set(json.load(f))
        except Exception as e:
            logger.error(f"Failed to load processed log, starting fresh: {e}")
            return set()
    return set()

def save_processed_files(processed_files):
    """Save processed file keys into JSON log file."""
    try:
        with open(PROCESSED_LOG, "w") as f:
            json.dump(list(processed_files), f)
    except Exception as e:
        logger.error(f"Failed to save processed files log: {e}")

def is_already_processed(file_key, processed_files):
    """Check if file_key already exists in processed set."""
    return file_key in processed_files

def mark_as_processed(file_key, processed_files):
    """Mark file_key as processed and save."""
    processed_files.add(file_key)
    save_processed_files(processed_files)



def download_zip_from_url(url, save_path):
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded ZIP: {save_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download ZIP from {url}: {e}")
        return False


def extract_zip(zip_path, extract_to, password):
    try:
        with pyzipper.AESZipFile(zip_path) as zf:
            zf.pwd = password
            zf.extractall(extract_to)
        logger.info(f"Extracted ZIP: {zip_path} -> {extract_to}")
        return True
    except Exception as e:
        logger.error(f"Failed to extract ZIP: {zip_path}, error: {e}")
        return False


def process_dbf_download_links():
    today = datetime.today()
    today_str = today.strftime('%d-%b-%Y')
    tomorrow_str = (today + timedelta(days=1)).strftime('%d-%b-%Y')

    base_zip_folder = os.path.join(ATTACHMENT_FOLDER, "zips", today_str)
    base_extract_folder = os.path.join(ATTACHMENT_FOLDER, "extracted", today_str)
    kfintech_folder = os.path.join(ATTACHMENT_FOLDER, "kfintech", today_str)
    cams_folder = os.path.join(ATTACHMENT_FOLDER, "cams", today_str)

    os.makedirs(base_zip_folder, exist_ok=True)
    os.makedirs(base_extract_folder, exist_ok=True)
    os.makedirs(kfintech_folder, exist_ok=True)
    os.makedirs(cams_folder, exist_ok=True)

    all_files = []
    processed_files = load_processed_files()   
    url_counter = 0

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        search_criteria = f'(SINCE "{today_str}" BEFORE "{tomorrow_str}")'
        status, messages = mail.search(None, search_criteria)

        if status != "OK" or not messages or not messages[0]:
            logger.warning(f"No emails found today: {today_str}")
            mail.logout()
            return []

        email_ids = messages[0].split()

        for eid in email_ids:
            try:
                res, msg_data = mail.fetch(eid, "(RFC822)")
                if res != 'OK':
                    logger.error(f"Failed to fetch email {eid}")
                    continue
                
                # Handle different email data formats safely
                msg_raw = None
                if msg_data and msg_data[0]:
                    if isinstance(msg_data[0], tuple) and len(msg_data[0]) > 1:
                        # Standard format: (b'RFC822 data', b'...')
                        msg_raw = msg_data[0][1]
                    elif isinstance(msg_data[0], bytes):
                        # Alternative format: raw bytes
                        msg_raw = msg_data[0]
                    elif isinstance(msg_data[0], tuple) and len(msg_data[0]) == 1:
                        # Tuple with only one element
                        msg_raw = msg_data[0][0]
                
                if not msg_raw:
                    logger.warning(f"Could not extract email data for {eid}")
                    continue
                
                msg = email.message_from_bytes(msg_raw)
                subject_header = decode_header(msg.get("Subject"))
                if subject_header and subject_header[0]:
                    subject, encoding = subject_header[0]
                    subject = subject.decode(encoding or "utf-8") if isinstance(subject, bytes) else subject
                else:
                    subject = "No Subject"
                
                logger.info(f"Processing email: {subject}")

                body, plain_text = "", ""

                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            body_content = part.get_payload(decode=True)
                            if body_content:
                                body = body_content.decode(errors="ignore")
                        elif part.get_content_type() == "text/plain":
                            text_content = part.get_payload(decode=True)
                            if text_content:
                                plain_text = text_content.decode(errors="ignore")
                else:
                    body_content = msg.get_payload(decode=True)
                    if body_content:
                        body = body_content.decode(errors="ignore")
                        plain_text = body

                soup = BeautifulSoup(body, 'html.parser')

                extra_fields = {}
                table = soup.find('table')
                if table:
                    for row in table.find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            key = cols[0].get_text(strip=True)
                            value = cols[1].get_text(strip=True)
                            extra_fields[key] = value
                    logger.info(f"Parsed extra fields: {extra_fields}")

                file_type = extra_fields.get("File Type", "").upper()
                logger.info(f"File Type: {file_type}")

                
                if file_type == "CSVWH":
                    final_password = b"12121212"
                elif "camsonline.com" in body.lower():  
                    final_password = b"123456"
                else:
                    pass_match = re.search(r'pass\W*([A-Za-z0-9@!#\$%&\*\-_]+)', plain_text, re.I)
                    if pass_match:
                        final_password = pass_match.group(1).encode()
                    else:
                        final_password = b"123456"

                unique_urls = []
                cams_matches = re.findall(r'https://mailback\d*\.camsonline\.com/[^\s"<>]+\.zip', body, flags=re.IGNORECASE)
                unique_urls.extend(cams_matches)

                if "click here" in body.lower():
                    kfin_match = soup.find('a', string=re.compile(r'Click Here', re.I))
                    if kfin_match and kfin_match.has_attr('href'):
                        kfin_url = kfin_match['href']
                        unique_urls.append(kfin_url)
                        if not pass_match:
                            final_password = b"Mutual@11"

                unique_urls = list(set(unique_urls))

         
                for part in msg.walk():
                    if part.get_content_disposition() != "attachment":
                        continue 
                    filename = part.get_filename()
                    if not filename or not filename.lower().endswith(".zip"):
                        continue

                    file_key = f"{filename}_{subject}"
                    if is_already_processed(file_key, processed_files):
                        logger.info(f"Skipping already processed file: {file_key}")
                        continue

                    zip_path = os.path.join(cams_folder, filename)
                    with open(zip_path, "wb") as f:
                        f.write(part.get_payload(decode=True))
                    logger.info(f"Downloaded CAMS ZIP attachment: {zip_path}")

                    extract_folder = os.path.join(cams_folder, os.path.splitext(filename)[0])
                    os.makedirs(extract_folder, exist_ok=True)

                    if extract_zip(zip_path, extract_folder, final_password):
                        for file in os.listdir(extract_folder):
                            file_path = os.path.join(extract_folder, file)
                            try:
                                if file.lower().endswith('.dbf'):
                                    table = DBF(file_path, load=True, encoding='latin1')
                                    rows = [dict(rec) for rec in table]
                                elif file.lower().endswith('.csv'):
                                    df = pd.read_csv(file_path)
                                    rows = df.to_dict(orient='records')
                                else:
                                    continue

                                mark_as_processed(file_key, processed_files)
                                all_files.append({
                                    "file": file, "rows": rows, "subject": subject,
                                    "file_type": file_type, "password": final_password.decode(),
                                    "source": "CAMS Attachment"
                                })
                            except Exception as e:
                                logger.error(f"Failed to read file: {file}, error: {e}")

           
                for url in unique_urls:
                    url_counter += 1
                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                    zip_filename = f"download_{timestamp}_{url_counter}.zip"

                    is_kfintech = "kfintech" in url.lower()
                    zip_folder = kfintech_folder if is_kfintech else base_zip_folder
                    extract_folder = kfintech_folder if is_kfintech else base_extract_folder

                    file_key = f"{subject}_{url}"
                    if is_already_processed(file_key, processed_files):
                        logger.info(f"Skipping already processed URL file: {file_key}")
                        continue

                    zip_path = os.path.join(zip_folder, zip_filename)
                    extract_subfolder = os.path.join(extract_folder, os.path.splitext(zip_filename)[0])
                    os.makedirs(extract_subfolder, exist_ok=True)

                    if download_zip_from_url(url, zip_path):
                        if extract_zip(zip_path, extract_subfolder, final_password):
                            for file in os.listdir(extract_subfolder):
                                file_path = os.path.join(extract_subfolder, file)
                                try:
                                    if file.lower().endswith('.dbf'):
                                        table = DBF(file_path, load=True, encoding='latin1')
                                        rows = [dict(rec) for rec in table]
                                    elif file.lower().endswith('.csv'):
                                        df = pd.read_csv(file_path)
                                        rows = df.to_dict(orient='records')
                                    else:
                                        continue

                                    mark_as_processed(file_key, processed_files)
                                    all_files.append({
                                        "file": file, "rows": rows, "subject": subject,
                                        "file_type": file_type, "password": final_password.decode(),
                                        "url": url, "extra_fields": extra_fields
                                    })
                                except Exception as e:
                                    logger.error(f"Failed to read file: {file}, error: {e}")
            
            except Exception as e:
                logger.error(f"Error processing email {eid}: {e}")
                continue

        mail.logout()

    except Exception as e:
        logger.exception(f"process_dbf_download_links failed: {e}")

    logger.info(f"Total files processed this run: {len(all_files)}")
    return all_files