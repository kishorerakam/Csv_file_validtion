import csv
import os
import shutil
import logging
from collections import Counter
from typing import List

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

import paramiko  # Required for SFTP upload

app = FastAPI()

# Serve static HTML UI
app.mount("/ui", StaticFiles(directory="."), name="ui")

# Logging setup
logging.basicConfig(level=logging.INFO)

# Local destination folder
DESTINATION_DIR = "validated_files"
os.makedirs(DESTINATION_DIR, exist_ok=True)  # Ensure it exists

# SFTP Config (Change these to your credentials when needed)
SFTP_HOST = "your.sftp.server.com"
SFTP_PORT = 22
SFTP_USER = "your_username"
SFTP_PASS = "your_password"
SFTP_REMOTE_DIR = "/remote/validated/"

# 1. Detect delimiter from header
def get_delimiter_from_header(header_line):
    for delim in [',', '|', '\t', ';']:
        if delim in header_line:
            return delim
    return ','

# 2. Count delimiter occurrence for each row
def count_delimiters_in_file(file_path, delimiter):
    counts = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            counts.append(len(row) - 1)
    return counts

# 3. Core file validation logic
def check_csv_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    if not lines:
        return {"status": "Failed", "reason": "File is empty."}

    header_line = lines[0].strip()
    delimiter = get_delimiter_from_header(header_line)
    expected_delimiters = header_line.count(delimiter)

    mismatches = []
    nulls = []

    for i, line in enumerate(lines[1:], start=2):  # line 1 is header
        row = line.strip()
        fields = row.split(delimiter)
        actual_delimiters = len(fields) - 1

        if actual_delimiters != expected_delimiters:
            issue_type = "fewer" if actual_delimiters < expected_delimiters else "extra"
            mismatches.append({
                "row": i,
                "content": row,
                "issue": f"{issue_type} delimiters ({actual_delimiters}) than expected ({expected_delimiters})"
            })

        if any(field.strip() == "" for field in fields):
            nulls.append({
                "row": i,
                "content": row,
                "issue": "contains empty (null) value"
            })

    return {
        "delimiter": delimiter,
        "expected_delimiters": expected_delimiters,
        "total_data_rows": len(lines) - 1,
        "row_issues": mismatches + nulls,
        "status": "Failed" if mismatches or nulls else "Successful"
    }

# 4. SFTP Upload Function
def upload_via_sftp(local_path, remote_path, host, user, password, port=22):
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(local_path, remote_path)
        sftp.close()
        transport.close()
        logging.info(f"Uploaded {local_path} to {remote_path} on {host}")
    except Exception as e:
        logging.error(f"SFTP upload failed for {local_path}: {e}")

# 5. Main upload and validate endpoint
@app.post("/validate_csv/")
async def validate(files: List[UploadFile] = File(...)):
    all_results = []

    for file in files:
        temp_path = f"temp_{file.filename}"
        with open(temp_path, "wb") as f_out:
            shutil.copyfileobj(file.file, f_out)

        result = check_csv_file(temp_path)
        result["filename"] = file.filename
        all_results.append(result)

        if result["status"] == "Successful":
            try:
                # Copy to local validated folder
                dest_path = os.path.join(DESTINATION_DIR, file.filename)
                shutil.copy(temp_path, dest_path)
                logging.info(f"Copied to local folder: {dest_path}")

                # Upload via SFTP
                remote_path = os.path.join(SFTP_REMOTE_DIR, file.filename)
                upload_via_sftp(temp_path, remote_path, SFTP_HOST, SFTP_USER, SFTP_PASS)

            except Exception as e:
                logging.error(f"Post-validation copy or upload failed for {file.filename}: {e}")
        else:
            logging.info(f"File {file.filename} failed validation. Skipping copy/upload.")

        try:
            os.remove(temp_path)
        except Exception as e:
            logging.warning(f"Failed to delete temp file {file.filename}: {e}")

    return {"results": all_results}

# 6. Serve HTML UI
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    with open("csv_validator_ui.html", "r", encoding="utf-8") as f:
        return f.read()
