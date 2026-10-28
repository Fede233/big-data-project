"""Provided support: CSV input/output, normalisation and shared definitions.

read_csv returns string tuples; interpretation of their contents comes later.
"""

import csv
from datetime import datetime
from io import StringIO
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_START = datetime(2010, 1, 1)
ANALYSIS_END = datetime(2011, 12, 1)
TRANSACTION_COLUMNS = (
    "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
    "UnitPrice", "CustomerID", "Country",
)
CUSTOMER_COLUMNS = ("CustomerID", "Segment", "ServicePlan")
EMPLOYEE_COLUMNS = ("EmployeeID", "EmployeeName", "Team", "Role")
ASSIGNMENT_COLUMNS = ("CustomerID", "EmployeeID")


def parse_csv_line(line):
    """Read quoted CSV safely: a comma inside a quoted description is not a separator."""
    # A malformed row remains a record, so the quality step can count it.
    try:
        return tuple(next(csv.reader([line], strict=True)))
    except csv.Error:
        return (line,)


def skip_header(partition_index, lines):
    # Remove only the first physical line, not rows that resemble the header.
    if partition_index == 0:
        next(lines, None)
    return lines


def read_csv(sc, path, expected_columns):
    """Check the header and return a distributed RDD of data-field tuples."""
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")
    linesRDD = sc.textFile(str(path), minPartitions=2)
    # take(1) returns only the header to Python; the full file stays distributed.
    first_lines = linesRDD.take(1)
    if not first_lines:
        raise ValueError(f"Missing CSV header: {path.name}")
    header = parse_csv_line(first_lines[0].lstrip("\ufeff"))
    if header != tuple(expected_columns):
        raise ValueError(f"Unexpected CSV header in {path.name}: {header}")
    # Partition zero contains the file start. Other partitions have no header to skip.
    dataLinesRDD = linesRDD.mapPartitionsWithIndex(skip_header)
    rowsRDD = dataLinesRDD.map(parse_csv_line)
    # Each record is a tuple of CSV fields, still stored as strings.
    return rowsRDD


def normalise_transaction(row):
    """Trim fields and standardise the cancellation prefix without dropping rows."""
    if len(row) != len(TRANSACTION_COLUMNS):
        return row
    fields = list(row)
    for index in (0, 1, 3, 4, 5, 6, 7):
        fields[index] = fields[index].strip()
    if fields[0].startswith("c"):
        fields[0] = "C" + fields[0][1:]
    # Description is preserved; missing Country remains empty in the raw tuple.
    return tuple(fields)


def format_csv_row(row):
    """Build one escaped CSV line in memory; Spark adds its final newline."""
    stream = StringIO()
    csv.writer(stream, lineterminator="\n").writerow(row)
    return stream.getvalue()[:-1]


def add_csv_header(rows, columns):
    """Yield a header followed by records for one output partition."""
    yield format_csv_row(columns)
    for row in rows:
        yield format_csv_row(row)


def write_csv(rowsRDD, destination, columns):
    """Save one CSV part per partition, preserving any previous output directory."""
    destination = Path(destination).resolve()
    if destination.exists():
        raise FileExistsError(f"Output already exists; choose a new output root: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    csvLinesRDD = rowsRDD.mapPartitions(lambda rows: add_csv_header(rows, columns))
    csvLinesRDD.saveAsTextFile(str(destination))
