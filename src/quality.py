"""Provided support: transaction classification, input validation and quality counts."""

from datetime import datetime
from decimal import Decimal, InvalidOperation

from common import ANALYSIS_END, ANALYSIS_START, normalise_transaction


FINANCIAL_CLASSES = ("Sale", "Cancellation", "OtherNegativeAdjustment")
CLASS_CHECKS = {
    "Invalid": "invalid_rows", "Excluded": "excluded_rows", "Sale": "sale_rows",
    "Cancellation": "cancellation_rows",
    "OtherNegativeAdjustment": "other_negative_adjustment_rows",
}
ROW_CHECKS = (
    "total_transaction_rows", "invalid_rows", "excluded_rows", "sale_rows",
    "cancellation_rows", "other_negative_adjustment_rows",
    "excluded_nonpositive_price", "excluded_zero_quantity",
    "excluded_positive_cancellation_quantity", "included_rows",
    "missing_customer_included", "missing_country_included",
    "included_inside_period", "included_outside_period",
)


def classify_transaction(row):
    """Return (class, reason, normalised strings, typed transaction or None).

    Typed fields: InvoiceNo, StockCode, Description, Quantity (int),
    InvoiceDate (datetime), UnitPrice (Decimal), CustomerID, Country.
    """
    row = normalise_transaction(row)
    if len(row) != 8:
        return ("Invalid", "invalid_columns", row, None)
    if not row[0] or not row[1]:
        return ("Invalid", "missing_required_identifier", row, None)
    try:
        quantity = Decimal(row[3])
        price = Decimal(row[5])
        invoice_date = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")
        if not quantity.is_finite() or quantity != quantity.to_integral_value():
            return ("Invalid", "invalid_quantity", row, None)
        if not price.is_finite():
            return ("Invalid", "invalid_price", row, None)
        quantity = int(quantity)
    except (InvalidOperation, ValueError):
        return ("Invalid", "invalid_numeric_or_date", row, None)

    transaction = (row[0], row[1], row[2], quantity, invoice_date, price, row[6], row[7] or "UNKNOWN")
    if price <= 0:
        return ("Excluded", "nonpositive_price", row, transaction)
    if quantity == 0:
        return ("Excluded", "zero_quantity", row, transaction)
    if row[0].startswith("C") and quantity > 0:
        return ("Excluded", "positive_cancellation_quantity", row, transaction)
    if quantity > 0:
        return ("Sale", "", row, transaction)
    if row[0].startswith("C"):
        return ("Cancellation", "", row, transaction)
    return ("OtherNegativeAdjustment", "", row, transaction)


# List the counters affected by one classified transaction.
def row_checks(record):
    category, reason, raw_row, transaction = record
    checks = [("total_transaction_rows", 1), (CLASS_CHECKS[category], 1)]
    if category == "Excluded":
        checks.append(("excluded_" + reason, 1))
    if category in FINANCIAL_CLASSES:
        checks.append(("included_rows", 1))
        if not transaction[6]:
            checks.append(("missing_customer_included", 1))
        if not raw_row[7]:
            checks.append(("missing_country_included", 1))
        if ANALYSIS_START <= transaction[4] < ANALYSIS_END:
            checks.append(("included_inside_period", 1))
        else:
            checks.append(("included_outside_period", 1))
    return checks


# Check required fields and unique primary keys in one table.
def validate_dimension(rowsRDD, name, field_count):
    malformedRowsRDD = rowsRDD.filter(lambda row: len(row) != field_count or any(not value for value in row))
    if malformedRowsRDD.take(1):
        raise ValueError(f"{name}: wrong field count or empty required field")
    keysRDD = rowsRDD.map(lambda row: (row[0], 1))
    # key=primary identifier, value=1 per input row
    keyCountsRDD = keysRDD.reduceByKey(lambda a, b: a + b)
    # key=primary identifier, value=number of rows with that identifier
    duplicateKeysRDD = keyCountsRDD.filter(lambda pair: pair[1] > 1)
    duplicates = duplicateKeysRDD.take(1)
    if duplicates:
        raise ValueError(f"{name}: duplicate key {duplicates[0][0]}")


# Validate the three tables and their customer/employee links.
def validate_dimensions(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD):
    validate_dimension(customersRDD, "customers", 3)
    validate_dimension(employeesRDD, "employees", 4)
    validate_dimension(assignmentsRDD, "customer_assignments", 2)

    invalidProfilesRDD = customersRDD.filter(
        lambda row: row[1] not in ("Retail", "Wholesale") or row[2] not in ("Standard", "Priority")
    )
    invalidEmployeesRDD = employeesRDD.filter(
        lambda row: row[2] not in ("TeamA", "TeamB", "TeamC")
        or row[3] not in ("AccountManager", "SeniorAccountManager")
    )
    if invalidProfilesRDD.take(1) or invalidEmployeesRDD.take(1):
        raise ValueError("Invalid customer profile, employee team or employee role")

    customerKeysRDD = customersRDD.map(lambda row: (row[0], 1))
    # key=CustomerID, value=1 per customer profile
    employeeKeysRDD = employeesRDD.map(lambda row: (row[0], 1))
    # key=EmployeeID, value=1 per employee profile
    assignmentsByCustomerRDD = assignmentsRDD.map(lambda row: (row[0], row[1]))
    # key=CustomerID, value=EmployeeID; uniqueness has already been checked
    customerAssignmentsRDD = assignmentsByCustomerRDD.leftOuterJoin(customerKeysRDD)
    # key=CustomerID, value=(EmployeeID, 1 or None)
    if customerAssignmentsRDD.filter(lambda pair: pair[1][1] is None).take(1):
        raise ValueError("customer_assignments: unknown CustomerID")

    assignmentsByEmployeeRDD = assignmentsRDD.map(lambda row: (row[1], row[0]))
    # key=EmployeeID, value=CustomerID
    employeeAssignmentsRDD = assignmentsByEmployeeRDD.leftOuterJoin(employeeKeysRDD)
    # key=EmployeeID, value=(CustomerID, 1 or None)
    if employeeAssignmentsRDD.filter(lambda pair: pair[1][1] is None).take(1):
        raise ValueError("customer_assignments: unknown EmployeeID")

    sourceCustomerKeysRDD = classifiedTransactionsRDD.filter(
        lambda record: record[3] is not None and record[3][6] != ""
    )
    sourceCustomerKeysRDD = sourceCustomerKeysRDD.map(lambda record: (record[3][6], 1))
    sourceCustomerKeysRDD = sourceCustomerKeysRDD.distinct()
    # key=CustomerID from any parseable row, value=1 (one pair per customer)
    if sourceCustomerKeysRDD.subtractByKey(customerKeysRDD).take(1):
        raise ValueError("customers: a source CustomerID is missing")
    if customerKeysRDD.subtractByKey(sourceCustomerKeysRDD).take(1):
        raise ValueError("customers: CustomerID not present in parseable source records")

    unassignedCustomersRDD = customerKeysRDD.subtractByKey(assignmentsByCustomerRDD)
    return {
        "customers": customersRDD.count(), "employees": employeesRDD.count(),
        "assignments": assignmentsRDD.count(), "unassigned_customers": unassignedCustomersRDD.count(),
    }


# Combine transaction checks and dimension counts into one summary.
def quality_summary(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD):
    rowChecksRDD = classifiedTransactionsRDD.flatMap(row_checks)
    # key=check name, value=1
    checkCountsRDD = rowChecksRDD.reduceByKey(lambda a, b: a + b)
    # key=check name, value=total row count for that check
    checks = {}
    for check_name in ROW_CHECKS:
        checks[check_name] = 0
    for check_name, count in checkCountsRDD.collect():
        checks[check_name] = count
    # Only a few check totals reach the driver, never the transaction dataset.

    normalisedRowsRDD = classifiedTransactionsRDD.map(lambda record: record[2])
    checks["duplicate_row_excess"] = checks["total_transaction_rows"] - normalisedRowsRDD.distinct().count()

    invoiceCustomersRDD = normalisedRowsRDD.filter(
        lambda row: len(row) == 8 and row[0] != "" and row[6] != ""
    ).map(lambda row: (row[0], row[6])).distinct()
    # key=InvoiceNo, value=distinct CustomerID
    customerInvoiceCountsRDD = invoiceCustomersRDD.map(lambda pair: (pair[0], 1))
    # key=InvoiceNo, value=1 per distinct identified customer
    customerCountsPerInvoiceRDD = customerInvoiceCountsRDD.reduceByKey(lambda a, b: a + b)
    checks["conflicting_invoice_customers"] = customerCountsPerInvoiceRDD.filter(lambda pair: pair[1] > 1).count()

    invoiceCountriesRDD = normalisedRowsRDD.filter(
        lambda row: len(row) == 8 and row[0] != "" and row[7] != ""
    ).map(lambda row: (row[0], row[7])).distinct()
    # key=InvoiceNo, value=distinct non-empty Country
    countryInvoiceCountsRDD = invoiceCountriesRDD.map(lambda pair: (pair[0], 1))
    # key=InvoiceNo, value=1 per distinct non-empty country
    countryCountsPerInvoiceRDD = countryInvoiceCountsRDD.reduceByKey(lambda a, b: a + b)
    checks["conflicting_invoice_countries"] = countryCountsPerInvoiceRDD.filter(lambda pair: pair[1] > 1).count()

    dimension_checks = validate_dimensions(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD)
    checks.update(dimension_checks)
    class_total = 0
    for check_name in CLASS_CHECKS.values():
        class_total += checks[check_name]
    if class_total != checks["total_transaction_rows"]:
        raise AssertionError("The five classes do not partition the transaction rows")
    if checks["included_inside_period"] + checks["included_outside_period"] != checks["included_rows"]:
        raise AssertionError("The period counts do not partition the included rows")
    return checks
