"""Five provided checks for the analytical tasks, using small known examples."""

import csv
from decimal import Decimal
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from main import create_spark_context
from quality import classify_transaction
from task_1_sales import run_task_1
from task_2_products import run_task_2
from task_3_customers import run_task_3
from task_4_employees import run_task_4
from task_5_bonus import run_task_5, bonus_row


def transaction(invoice, code, quantity, price, customer="C1", country="UK", date="2010-01-01 00:00:00"):
    return (invoice, code, "Item", str(quantity), date, str(price), customer, country)


def read_output(path):
    rows = []
    for part in path.glob("part-*"):
        with part.open(encoding="utf-8", newline="") as stream:
            rows.extend(csv.DictReader(stream))
    return rows


class TaskTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sc = create_spark_context()
        cls.addClassCleanup(cls.sc.stop)
        temporary = TemporaryDirectory(prefix="task-tests-", dir=PROJECT_ROOT / ".cache")
        cls.addClassCleanup(temporary.cleanup)
        cls.output = Path(temporary.name)
        records = [
            transaction("S1", "A", 1, ".005"), transaction("S1", "A", 1, ".005"),
            transaction("C1", "A", -1, ".01", date="2010-02-01 00:00:00"),
            transaction("ADJ", "A", -1, ".01", date="2010-02-01 00:00:00"),
            transaction("C2", "A", -1, 5, "C4", "Germany"),
            transaction("S2", "A", 1, 10, "", "Germany", "2011-01-01 00:00:00"),
            transaction("S3", "A", 1, 10, "C2", "France", "2011-01-01 00:00:00"),
            transaction("S3", "B", 1, 10, "C2", "France", "2011-01-01 00:00:00"),
            transaction("S4", "A", 1, 10, "C2", "France", "2011-02-01 00:00:00"),
            transaction("S5", "A", 1, 10, "", "Italy", "2011-01-01 00:00:00"),
            transaction("S5", "B", 1, 10, "", "Italy", "2011-01-01 00:00:00"),
            transaction("BEFORE", "A", 1, 999, date="2009-12-31 23:59:59"),
            transaction("AFTER", "A", 1, 999, date="2011-12-01 00:00:00"),
        ]
        customers = [("C1", "Retail", "Standard"), ("C2", "Wholesale", "Priority"),
                     ("C3", "Retail", "Priority"), ("C4", "Wholesale", "Standard")]
        employees = [("E001", "One", "TeamA", "AccountManager"),
                     ("E002", "Two", "TeamA", "AccountManager"),
                     ("E003", "Three", "TeamB", "AccountManager"),
                     ("E012", "Empty", "TeamB", "AccountManager")]
        data = cls.sc.parallelize(records, 2).map(classify_transaction).cache()
        cls.addClassCleanup(data.unpersist)
        customersRDD = cls.sc.parallelize(customers, 2)
        employeesRDD = cls.sc.parallelize(employees, 2)
        assignmentsRDD = cls.sc.parallelize([("C1", "E001"), ("C2", "E002")], 2)
        run_task_1(data, cls.output)
        run_task_2(data, cls.output)
        run_task_3(data, customersRDD, cls.output)
        run_task_4(data, customersRDD, employeesRDD, assignmentsRDD, cls.output)
        run_task_5(data, employeesRDD, assignmentsRDD, cls.output)

    def test_sales_rounding_invoices_and_growth(self):
        monthly = {(r["Country"], r["Month"]): r for r in read_output(self.output / "task_1/country_monthly")}
        self.assertEqual(monthly["UK", "2010-01"]["GrossSales"], "0.02")
        self.assertEqual(monthly["UK", "2010-01"]["PurchaseInvoices"], "1")
        self.assertEqual(monthly["Germany", "2010-01"]["CancellationPct"], "")
        growth = {r["Country"]: r for r in read_output(self.output / "task_1/country_growth")}
        self.assertEqual(growth["Germany"]["NetSales2010"], "-5.00")
        self.assertEqual(growth["Germany"]["GrowthPct"], "")
        self.assertEqual(growth["Italy"]["NetSales2010"], "0.00")
        self.assertEqual(growth["Italy"]["GrowthPct"], "")

    def test_product_ties_and_watchlist_threshold(self):
        leaders = read_output(self.output / "task_2/country_product_leaders")
        italy = [r for r in leaders if r["Country"] == "Italy"]
        self.assertEqual({r["StockCode"] for r in italy}, {"A", "B"})
        self.assertTrue(all(r["Buyers"] == "0" for r in italy))
        records = [transaction("S" + code + str(i), code, 1, 100)
                   for code in ("E", "F") for i in range(10)]
        records.extend([transaction("CE", "E", -1, 100), transaction("CF", "F", -1, "99.99")])
        run_task_2(self.sc.parallelize(records, 2).map(classify_transaction), self.output / "threshold")
        watchlist = read_output(self.output / "threshold/task_2/product_watchlist")
        self.assertEqual({r["StockCode"] for r in watchlist}, {"E"})
        products = {r["StockCode"]: r for r in read_output(self.output / "threshold/task_2/product_summary")}
        self.assertEqual(products["F"]["CancellationPct"], "10.00")

    def test_customer_recurrence_and_inactive_profiles(self):
        customers = {r["CustomerID"]: r for r in read_output(self.output / "task_3/customer_summary")}
        self.assertEqual(customers["C1"]["PurchaseInvoices"], "1")
        self.assertEqual(customers["C2"]["PurchaseInvoices"], "2")
        self.assertEqual(customers["C2"]["IsRepeat"], "true")
        self.assertEqual(customers["C3"]["FirstPurchaseDate"], "")
        self.assertEqual(customers["C3"]["NetSales"], "0.00")
        self.assertEqual(customers["C4"]["FirstPurchaseDate"], "")
        self.assertEqual(customers["C4"]["NetSales"], "-5.00")
        segments = read_output(self.output / "task_3/customer_segments")
        inactive = next(r for r in segments if r["Segment"] == "Wholesale" and r["ServicePlan"] == "Standard")
        self.assertEqual(inactive["RepeatSharePct"], "")

    def test_employee_portfolios_and_reconciliation(self):
        portfolios = {r["EmployeeID"]: r for r in read_output(self.output / "task_4/employee_portfolios")}
        self.assertEqual(portfolios["E012"]["GrossPerAssignedCustomer"], "")
        self.assertEqual(portfolios["E002"]["RepeatCustomers"], "1")
        leaders = read_output(self.output / "task_4/team_leaders")
        self.assertEqual({r["EmployeeID"] for r in leaders if r["Team"] == "TeamB"}, {"E003", "E012"})
        totals = {r["Metric"]: r for r in read_output(self.output / "task_4/sales_reconciliation")}
        self.assertEqual(len(totals), 4)
        self.assertTrue(all(r["Difference"] == "0.00" for r in totals.values()))
        self.assertEqual(totals["Adjustments"]["OverallTotal"], "0.01")
        self.assertEqual(totals["NetSales"]["OverallTotal"], "55.00")
        self.assertEqual(totals["NetSales"]["AssignedTotal"], "30.00")
        self.assertEqual(totals["NetSales"]["UnassignedTotal"], "-5.00")
        self.assertEqual(totals["NetSales"]["AnonymousTotal"], "30.00")

    def test_bonus_policy_and_complete_months(self):
        rows = read_output(self.output / "task_5/employee_monthly_bonus")
        self.assertEqual(len(rows), 92)
        self.assertEqual(len({(r["EmployeeID"], r["Month"]) for r in rows}), 92)
        self.assertTrue(all(r["BonusStatus"] == "NO_SALES" for r in rows if r["EmployeeID"] == "E012"))
        february = next(r for r in rows if r["EmployeeID"] == "E001" and r["Month"] == "2010-02")
        self.assertEqual((february["NetSales"], february["CancellationPct"], february["BonusStatus"]), ("-0.02", "", "NO_SALES"))
        cases = [
            ("16000", "1000", "0", "ELIGIBLE", "150.00"),
            ("15000", "2000", "0", "HIGH_CANCELLATIONS", "0.00"),
            ("80000", "0", "5000", "ELIGIBLE", "500.00"),
            ("5000", "0", "0", "TARGET_NOT_MET", "0.00"),
            ("10000", "0", "0", "ELIGIBLE", "100.00"),
            ("9999.99", "0", "0", "TARGET_NOT_MET", "0.00"),
            ("20000", "2000", "0", "ELIGIBLE", "180.00"),
            ("20000", "2000.01", "0", "HIGH_CANCELLATIONS", "0.00"),
            ("9000", "2000", "0", "TARGET_NOT_MET", "0.00"),
            ("0", "50", "0", "NO_SALES", "0.00"),
            ("10000.50", "0", "0", "ELIGIBLE", "100.01"),
            ("50000.01", "0", "0", "ELIGIBLE", "500.00"),
        ]
        for gross, cancellations, adjustments, status, amount in cases:
            with self.subTest(gross=gross, cancellations=cancellations, adjustments=adjustments):
                row = bonus_row((("E001", "2010-01"), (1, tuple(Decimal(v) for v in (gross, cancellations, adjustments)))))
                self.assertEqual(row[-2:], (status, amount))


if __name__ == "__main__":
    unittest.main()
