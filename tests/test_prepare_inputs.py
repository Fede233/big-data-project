"""Four provided checks for the input-preparation tool.

Temporary examples are created inside the project and removed after each test.
"""

import csv
from datetime import datetime
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
from prepare_inputs import (
    generate_dimensions,
    is_parseable_record,
    keep_source_row,
    normalise_cell,
    normalise_identifier,
    write_csv,
)


class InputPreparationTests(unittest.TestCase):
    def test_sheet_overlap_uses_one_source_at_the_exact_month_boundary(self):
        self.assertTrue(keep_source_row("Year 2009-2010", "2010-11-30 23:59:59"))
        self.assertFalse(keep_source_row("Year 2009-2010", "2010-12-01 00:00:00"))
        self.assertFalse(keep_source_row("Year 2009-2010", "2010-12-09 20:01:00"))
        self.assertTrue(keep_source_row("Year 2010-2011", "2010-12-01 08:26:00"))

    def test_csv_preserves_commas_quotes_missing_values_and_single_line_records(self):
        with TemporaryDirectory(prefix=".input-test-", dir=PROJECT_ROOT) as temporary:
            path = Path(temporary) / "example.csv"
            description = normalise_cell('Gift, "large"\r\nblue')
            write_csv(path, ["Description", "CustomerID"], [[description, normalise_cell(None)]])
            self.assertEqual(len(path.read_text().splitlines()), 2)
            with path.open(newline="") as stream:
                rows = list(csv.reader(stream))
            self.assertEqual(rows[1], ['Gift, "large" blue', ""])
        self.assertEqual(normalise_identifier(12345.0), "12345")
        self.assertEqual(normalise_identifier(" 00123A "), "00123A")
        self.assertEqual(normalise_cell(datetime(2010, 1, 2, 9, 30)), "2010-01-02 09:30:00")

    def test_customer_population_uses_parseable_records_without_financial_filtering(self):
        row = ["C100", "A1", "", "-2", "2010-01-02 09:30:00", "0", "12345", ""]
        self.assertTrue(is_parseable_record(row))
        for index, bad_value in [(0, ""), (1, ""), (3, "1.5"), (3, "NaN"),
                                 (4, "2010-02-30 09:30:00"), (5, "Infinity")]:
            invalid_row = row.copy()
            invalid_row[index] = bad_value
            self.assertFalse(is_parseable_record(invalid_row))

    def test_dimensions_repeat_exactly_and_respect_keys_and_empty_portfolio(self):
        with TemporaryDirectory(prefix=".input-test-", dir=PROJECT_ROOT) as temporary:
            first = Path(temporary) / "first"
            second = Path(temporary) / "second"
            first.mkdir()
            second.mkdir()
            customer_ids = [str(customer_id) for customer_id in range(10000, 10040)]
            summary = generate_dimensions(customer_ids, first, 42)
            generate_dimensions(list(reversed(customer_ids)), second, 42)
            for path in first.iterdir():
                self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
            with (first / "employees.csv").open(newline="") as stream:
                employees = list(csv.DictReader(stream))
            with (first / "customers.csv").open(newline="") as stream:
                customers = list(csv.DictReader(stream))
            with (first / "customer_assignments.csv").open(newline="") as stream:
                assignments = list(csv.DictReader(stream))
            employee_ids = {row["EmployeeID"] for row in employees}
            self.assertEqual(len(employees), 12)
            self.assertEqual(len(employee_ids), 12)
            self.assertEqual(len(customers), 40)
            self.assertEqual(len({row["CustomerID"] for row in customers}), 40)
            self.assertEqual(len(assignments), 38)
            self.assertEqual(len({row["CustomerID"] for row in assignments}), 38)
            self.assertEqual(summary["unassigned_customers"], 2)
            self.assertTrue(all(row["CustomerID"] in customer_ids for row in assignments))
            self.assertTrue(all(row["EmployeeID"] in employee_ids - {"E012"} for row in assignments))
            self.assertEqual(summary["employee_assignment_counts"]["E012"], 0)
            for team in ("TeamA", "TeamB", "TeamC"):
                self.assertEqual(sum(row["Team"] == team for row in employees), 4)


if __name__ == "__main__":
    unittest.main()
