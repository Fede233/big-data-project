"""Acquire the source workbook and prepare the four project inputs."""

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import platform
import random
import shutil
import ssl
from tempfile import TemporaryDirectory
from urllib.request import urlopen
from zipfile import ZipFile

import certifi
import openpyxl


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PAGE = "https://archive.ics.uci.edu/dataset/502/online+retail+ii"
DOWNLOAD_URL = "https://archive.ics.uci.edu/static/public/502/online%2Bretail%2Bii.zip"
WORKBOOK_NAME = "online_retail_II.xlsx"
WORKBOOK_SHA256 = "bcbe73b35f5b7babf197fb0cb983a11f5d9ff929078d4aa53d171b1f2df2e980"
SOURCE_COLUMNS = [
    "Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price",
    "Customer ID", "Country",
]
TRANSACTION_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
    "UnitPrice", "CustomerID", "Country",
]
INPUT_NAMES = [
    "transactions.csv", "customers.csv", "employees.csv", "customer_assignments.csv",
]
OVERLAP_BOUNDARY = "2010-12-01 00:00:00"


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def acquire_workbook(raw_dir, allow_download):
    workbook_path = raw_dir / WORKBOOK_NAME
    if not workbook_path.exists():
        if not allow_download:
            raise FileNotFoundError("Source missing. Run again with --download.")
        raw_dir.mkdir(parents=True, exist_ok=True)
        # Extract only the expected workbook; archive paths are never trusted.
        with TemporaryDirectory(prefix="download-", dir=raw_dir) as temporary:
            archive_path = Path(temporary) / "source.zip"
            print("Downloading the official UCI archive...", flush=True)
            context = ssl.create_default_context(cafile=certifi.where())
            with urlopen(DOWNLOAD_URL, timeout=120, context=context) as response:
                with archive_path.open("wb") as destination:
                    shutil.copyfileobj(response, destination)
            with ZipFile(archive_path) as archive:
                with archive.open(WORKBOOK_NAME) as source:
                    temporary_workbook = Path(temporary) / WORKBOOK_NAME
                    with temporary_workbook.open("wb") as destination:
                        shutil.copyfileobj(source, destination)
            if checksum(temporary_workbook) != WORKBOOK_SHA256:
                raise ValueError("Unexpected source checksum; inspect the new source version.")
            temporary_workbook.replace(workbook_path)
    if checksum(workbook_path) != WORKBOOK_SHA256:
        raise ValueError("Local source checksum differs from the verified workbook.")
    return workbook_path


def normalise_cell(value):
    """Preserve missing/invalid values and make each CSV record one physical line."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        return format(Decimal(str(value)), "f")
    return str(value).replace("\r\n", " ").replace("\r", " ").replace("\n", " ")


def normalise_identifier(value):
    text = normalise_cell(value).strip()
    # Convert numeric Excel IDs to strings without changing alphanumeric codes.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = Decimal(str(value))
        if number.is_finite() and number == number.to_integral_value():
            return str(int(number))
    return text


def is_parseable_record(row):
    """Select the customer-ID population; financial classification comes later."""
    try:
        quantity = Decimal(row[3])
        price = Decimal(row[5])
        datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")
        return bool(row[0] and row[1]) and quantity.is_finite() and (
            quantity == quantity.to_integral_value() and price.is_finite()
        )
    except (InvalidOperation, ValueError):
        return False


def keep_source_row(sheet_name, invoice_date):
    # The second sheet owns December 2010; its rows also occur in the first.
    return not (sheet_name == "Year 2009-2010" and invoice_date >= OVERLAP_BOUNDARY)


def convert_transactions(workbook_path, destination):
    customer_ids = set()
    sheet_counts = {}
    retained_sheet_counts = {}
    missing = Counter()
    prepared_missing = Counter()
    normalisations = Counter()
    first_date = None
    last_date = None
    workbook = openpyxl.load_workbook(workbook_path, read_only=True, data_only=False)
    try:
        with destination.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(TRANSACTION_COLUMNS)
            for sheet in workbook:
                rows = sheet.iter_rows(values_only=True)
                header = list(next(rows))
                if header != SOURCE_COLUMNS:
                    raise ValueError(f"Unexpected columns in {sheet.title}: {header}")
                count = 0
                retained_count = 0
                for values in rows:
                    if len(values) != len(SOURCE_COLUMNS):
                        raise ValueError(f"Unexpected row width in {sheet.title}")
                    # Formulas are data, never executed or replaced with cached values.
                    row = [normalise_cell(value) for value in values]
                    for index in (0, 1, 6):
                        row[index] = normalise_identifier(values[index])
                    if row[0].startswith("c"):
                        row[0] = "C" + row[0][1:]
                    for index, value in enumerate(values):
                        if isinstance(value, str) and ("\n" in value or "\r" in value):
                            normalisations["cells_with_line_breaks"] += 1
                        if row[index] != normalise_cell(value):
                            normalisations["identifier_cells_normalised"] += 1
                    for column, value in zip(TRANSACTION_COLUMNS, row):
                        if not value.strip():
                            missing[column] += 1
                    if isinstance(values[4], datetime):
                        first_date = min(first_date, values[4]) if first_date else values[4]
                        last_date = max(last_date, values[4]) if last_date else values[4]
                    if row[6] and is_parseable_record(row):
                        customer_ids.add(row[6])
                    count += 1
                    if not keep_source_row(sheet.title, row[4]):
                        continue
                    writer.writerow(row)
                    retained_count += 1
                    for column, value in zip(TRANSACTION_COLUMNS, row):
                        if not value.strip():
                            prepared_missing[column] += 1
                sheet_counts[sheet.title] = count
                retained_sheet_counts[sheet.title] = retained_count
                print(f"Read {sheet.title}: {count:,} rows; retained {retained_count:,}", flush=True)
    finally:
        workbook.close()
    return sorted(customer_ids), {
        "sheet_rows": sheet_counts,
        "source_rows": sum(sheet_counts.values()),
        "retained_sheet_rows": retained_sheet_counts,
        "transaction_rows": sum(retained_sheet_counts.values()),
        "cross_sheet_overlap_rows_removed": sum(sheet_counts.values()) - sum(retained_sheet_counts.values()),
        "first_source_date": first_date.isoformat(sep=" ") if first_date else None,
        "last_source_date": last_date.isoformat(sep=" ") if last_date else None,
        "missing_fields": dict(missing),
        "prepared_missing_fields": dict(prepared_missing),
        "normalisations": dict(normalisations),
    }


def write_csv(path, columns, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)


def generate_dimensions(customer_ids, destination, seed):
    if not customer_ids:
        raise ValueError("No customer IDs found in parseable source records.")
    customer_ids = sorted(customer_ids)
    rng = random.Random(seed)
    customers = []
    for customer_id in customer_ids:
        segment = "Retail" if rng.random() < 0.70 else "Wholesale"
        service_plan = "Standard" if rng.random() < 0.75 else "Priority"
        customers.append([customer_id, segment, service_plan])
    write_csv(destination / "customers.csv", ["CustomerID", "Segment", "ServicePlan"], customers)

    names = [
        "Alex Morgan", "Jamie Reed", "Taylor Brooks", "Casey Ellis",
        "Jordan Blake", "Riley Hayes", "Cameron Lane", "Avery Cole",
        "Morgan Wells", "Robin Parker", "Charlie Quinn", "Drew Bennett",
    ]
    employees = []
    for index, name in enumerate(names):
        team = ["TeamA", "TeamB", "TeamC"][index // 4]
        role = "SeniorAccountManager" if index % 4 == 0 else "AccountManager"
        employees.append([f"E{index + 1:03d}", name, team, role])
    write_csv(destination / "employees.csv", ["EmployeeID", "EmployeeName", "Team", "Role"], employees)

    unassigned_count = max(1, len(customer_ids) * 5 // 100)
    unassigned_ids = set(rng.sample(customer_ids, unassigned_count))
    eligible_employees = [employee[0] for employee in employees if employee[0] != "E012"]
    assignments = [
        [customer_id, rng.choice(eligible_employees)]
        for customer_id in customer_ids if customer_id not in unassigned_ids
    ]
    write_csv(destination / "customer_assignments.csv", ["CustomerID", "EmployeeID"], assignments)
    return {
        "customers": len(customers), "employees": len(employees),
        "assignments": len(assignments), "unassigned_customers": unassigned_count,
        "segment_counts": dict(Counter(row[1] for row in customers)),
        "service_plan_counts": dict(Counter(row[2] for row in customers)),
        "employee_assignment_counts": {
            employee[0]: sum(row[1] == employee[0] for row in assignments)
            for employee in employees
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true", help="Download the source if missing.")
    parser.add_argument("--overwrite", action="store_true", help="Replace prepared inputs, preserving the raw workbook.")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    data_dir = PROJECT_ROOT / "data"
    input_dir = data_dir / "inputs"
    manifest_path = data_dir / "input_manifest.json"
    existing = [input_dir / name for name in INPUT_NAMES if (input_dir / name).exists()]
    if (existing or manifest_path.exists()) and not args.overwrite:
        parser.error("Prepared inputs already exist. Use --overwrite to regenerate them.")
    workbook_path = acquire_workbook(data_dir / "raw", args.download)
    data_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="prepare-", dir=data_dir) as temporary:
        prepared_dir = Path(temporary)
        customer_ids, source_summary = convert_transactions(workbook_path, prepared_dir / "transactions.csv")
        dimension_summary = generate_dimensions(customer_ids, prepared_dir, args.seed)
        manifest = {
            "source": {
                "page": SOURCE_PAGE, "download_url": DOWNLOAD_URL,
                "doi": "10.24432/C5CG6D", "license": "CC BY 4.0",
                "attribution": "Chen, D. (2012). Online Retail II. UCI Machine Learning Repository.",
                "workbook_modified_at_utc": datetime.fromtimestamp(workbook_path.stat().st_mtime, timezone.utc).isoformat(),
                "workbook": f"raw/{WORKBOOK_NAME}", "bytes": workbook_path.stat().st_size,
                "sha256": checksum(workbook_path),
                "column_mapping": dict(zip(SOURCE_COLUMNS, TRANSACTION_COLUMNS)),
                "overlap_rule": "Keep first sheet before 2010-12-01 and second sheet from 2010-12-01; retain within-sheet duplicates",
            },
            "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
            "python": platform.python_version(), "openpyxl": openpyxl.__version__,
            "generation": {
                "seed": args.seed, "method": "Python random.Random; sorted CustomerID strings",
                "retail_probability": 0.70, "standard_probability": 0.75,
                "draw_order": "Customer profiles, unassigned sample, employee assignments",
                "roles": "First employee of each team is SeniorAccountManager; other roles AccountManager",
                "assignment_method": "Uniform choice among E001-E011; E012 remains empty",
            },
            "source_inspection": source_summary, "dimensions": dimension_summary,
            "inputs": {
                name: {"path": f"inputs/{name}", "bytes": (prepared_dir / name).stat().st_size,
                       "sha256": checksum(prepared_dir / name)}
                for name in INPUT_NAMES
            },
        }
        input_dir.mkdir(exist_ok=True)
        for name in INPUT_NAMES:
            (prepared_dir / name).replace(input_dir / name)
        temporary_manifest = prepared_dir / "input_manifest.json"
        temporary_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        temporary_manifest.replace(manifest_path)
    print(json.dumps({**source_summary, **dimension_summary}, indent=2))
    print("Prepared four input files. No analytical task results have been generated.")


if __name__ == "__main__":
    main()
