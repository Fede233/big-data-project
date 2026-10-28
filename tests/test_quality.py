"""Five provided checks for CSV handling and quality support.

Run these checks to verify the supplied setup before starting the tasks.
Small synthetic inputs and temporary outputs are removed after the tests.
"""

import csv
from decimal import Decimal
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from common import read_csv, write_csv
from main import create_spark_context
from quality import classify_transaction, quality_summary, validate_dimensions


# The input schema is fixed by the brief, independently of the solution.
TRANSACTION_COLUMNS = (
    "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
    "UnitPrice", "CustomerID", "Country",
)

def transaction(invoice="S1", customer="C001", country="UK", quantity="2", price="1.25", date="2010-01-01 00:00:00"):
    return (invoice, "A1", 'Gift, "blue"', quantity, date, price, customer, country)


CUSTOMERS = [("C001", "Retail", "Standard"), ("C002", "Wholesale", "Priority"), ("C003", "Retail", "Priority")]
EMPLOYEES = [("E001", "Alex", "TeamA", "AccountManager"), ("E002", "Jamie", "TeamB", "SeniorAccountManager")]
ASSIGNMENTS = [("C001", "E001")]
TRANSACTIONS = [
    transaction(), transaction(),
    transaction(customer="C002", country="France"),
    transaction(invoice="C2", customer="C002", quantity="-1", date="2011-12-01 00:00:00"),
    transaction(invoice="A1", country="", quantity="-1", price="3"),
    transaction(invoice="S2", customer="", country="", date="2009-12-31 23:59:59"),
    transaction(invoice="E1", customer="C003", price="0"),
    transaction(invoice="E2", quantity="0"),
    transaction(invoice="C3"),
    transaction(quantity="broken"), ("bad row",),
]


class TransactionParsingTests(unittest.TestCase):
    def test_classification_precedence_and_exact_types(self):
        cases = [
            (transaction(), "Sale", ""),
            (transaction(invoice="c2", quantity="-1"), "Cancellation", ""),
            (transaction(quantity="-1"), "OtherNegativeAdjustment", ""),
            (transaction(invoice="C3", quantity="0", price="0"), "Excluded", "nonpositive_price"),
            (transaction(invoice="C3", quantity="0"), "Excluded", "zero_quantity"),
            (transaction(invoice="C3"), "Excluded", "positive_cancellation_quantity"),
            (transaction(quantity="1.5"), "Invalid", None),
            (transaction(price="Infinity"), "Invalid", None),
            (transaction(quantity="NaN"), "Invalid", None),
            (transaction(date="2010-02-30 00:00:00"), "Invalid", None),
            (transaction(invoice=""), "Invalid", None),
        ]
        for row, category, reason in cases:
            with self.subTest(row=row):
                record = classify_transaction(row)
                self.assertEqual(record[0], category)
                if reason is not None:
                    self.assertEqual(record[1], reason)
        record = classify_transaction(transaction(customer="", country="", price="0.10"))
        self.assertEqual(record[3][3], 2)
        self.assertEqual(record[3][5], Decimal("0.10"))
        self.assertEqual(record[3][7], "UNKNOWN")
        self.assertEqual(record[2][7], "")


class QualityRDDTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sc = create_spark_context()
        cls.classifiedTransactionsRDD = cls.sc.parallelize(TRANSACTIONS, 3).map(classify_transaction).cache()

    @classmethod
    def tearDownClass(cls):
        cls.classifiedTransactionsRDD.unpersist()
        cls.sc.stop()

    def test_expected_summary_and_class_partition(self):
        checks = quality_summary(
            self.classifiedTransactionsRDD,
            self.sc.parallelize(CUSTOMERS, 2), self.sc.parallelize(EMPLOYEES, 2),
            self.sc.parallelize(ASSIGNMENTS, 2),
        )
        self.assertEqual(checks, {
            "total_transaction_rows": 11, "invalid_rows": 2, "excluded_rows": 3,
            "sale_rows": 4, "cancellation_rows": 1, "other_negative_adjustment_rows": 1,
            "excluded_nonpositive_price": 1, "excluded_zero_quantity": 1,
            "excluded_positive_cancellation_quantity": 1, "included_rows": 6,
            "missing_customer_included": 1, "missing_country_included": 2,
            "included_inside_period": 4, "included_outside_period": 2,
            "duplicate_row_excess": 1, "conflicting_invoice_customers": 1,
            "conflicting_invoice_countries": 1, "customers": 3, "employees": 2,
            "assignments": 1, "unassigned_customers": 2,
        })

    def test_duplicate_keys_and_missing_links_are_rejected(self):
        cases = [
            (CUSTOMERS + [CUSTOMERS[0]], EMPLOYEES, ASSIGNMENTS, "customers: duplicate key"),
            (CUSTOMERS, EMPLOYEES + [EMPLOYEES[0]], ASSIGNMENTS, "employees: duplicate key"),
            (CUSTOMERS, EMPLOYEES, ASSIGNMENTS * 2, "customer_assignments: duplicate key"),
            (CUSTOMERS, EMPLOYEES, [("C999", "E001")], "unknown CustomerID"),
            (CUSTOMERS, EMPLOYEES, [("C001", "E999")], "unknown EmployeeID"),
            (CUSTOMERS[:2], EMPLOYEES, ASSIGNMENTS, "source CustomerID is missing"),
            (CUSTOMERS + [("C999", "Retail", "Standard")], EMPLOYEES, ASSIGNMENTS, "not present in parseable source"),
        ]
        for customers, employees, assignments, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    validate_dimensions(
                        self.classifiedTransactionsRDD,
                        self.sc.parallelize(customers, 2), self.sc.parallelize(employees, 2),
                        self.sc.parallelize(assignments, 2),
                    )

    def test_csv_loading_preserves_quotes_bad_rows_and_only_skips_the_first_header(self):
        with TemporaryDirectory(prefix="quality-csv-", dir=PROJECT_ROOT / ".cache") as temporary:
            path = Path(temporary) / "transactions.csv"
            with path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(TRANSACTION_COLUMNS)
                writer.writerow(transaction())
                writer.writerow(TRANSACTION_COLUMNS)
                stream.write('\n"unclosed quote\n')
            rowsRDD = read_csv(self.sc, path, TRANSACTION_COLUMNS)
            records = rowsRDD.map(classify_transaction).collect()
            self.assertEqual(len(records), 4)
            self.assertEqual(sum(row[0] == "Invalid" for row in records), 3)
            self.assertEqual(records[0][3][2], 'Gift, "blue"')
            path.write_text("wrong,header\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Unexpected CSV header"):
                read_csv(self.sc, path, TRANSACTION_COLUMNS)
            path.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Missing CSV header"):
                read_csv(self.sc, path, TRANSACTION_COLUMNS)

    def test_csv_parts_have_headers_and_existing_outputs_are_protected(self):
        with TemporaryDirectory(prefix="quality-output-", dir=PROJECT_ROOT / ".cache") as temporary:
            path = Path(temporary) / "summary"
            expected = [("a,quoted", "1"), ('b"quoted', "2")]
            rowsRDD = self.sc.parallelize([(name, int(value)) for name, value in expected], 2)
            write_csv(rowsRDD, path, ("Check", "Count"))
            actual = []
            for part in sorted(path.glob("part-*")):
                with part.open(newline="") as stream:
                    reader = csv.reader(stream)
                    self.assertEqual(next(reader), ["Check", "Count"])
                    actual.extend(tuple(row) for row in reader)
            self.assertEqual(sorted(actual), sorted(expected))
            with self.assertRaises(FileExistsError):
                write_csv(rowsRDD, path, ("Check", "Count"))


if __name__ == "__main__":
    unittest.main()
