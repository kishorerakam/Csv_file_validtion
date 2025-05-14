import csv
import os
import shutil
import logging
from collections import Counter
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List

app = FastAPI()

app.mount("/ui", StaticFiles(directory="."), name="ui")

logging.basicConfig(level=logging.INFO)

def get_delimiter_from_header(header_line):
    for delim in [',', '|', '\t', ';']:
        if delim in header_line:
            return delim
    return ','

def count_delimiters_in_file(file_path, delimiter):
    counts = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            counts.append(len(row) - 1)
    return counts

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

    for i, line in enumerate(lines[1:], start=2):  # start=2 because line 1 is header
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

        try:
            os.remove(temp_path)
        except Exception as e:
            logging.warning(f"Failed to delete temp file {file.filename}: {e}")

    return {"results": all_results}

@app.get("/", response_class=HTMLResponse)
def serve_ui():
    with open("csv_validator_ui.html", "r", encoding="utf-8") as f:
        return f.read()
