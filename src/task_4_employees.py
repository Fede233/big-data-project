"""Task 4: employee portfolios, team leaders and sales reconciliation."""

from decimal import Decimal, ROUND_HALF_UP

from common import ANALYSIS_START, ANALYSIS_END, write_csv
from quality import FINANCIAL_CLASSES
from task_3_customers import customer_amounts


def customer_flags(pair):
    amounts, invoices = pair[1]
    invoices = invoices or 0
    return (pair[0], (1, 1 if invoices > 0 else 0, 1 if invoices >= 2 else 0,
                     amounts[0], amounts[1], amounts[2], amounts[0]))


def employee_metrics(pair):
    profile, totals = pair[1]
    if totals is None:
        totals = (0, 0, 0, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"))
    return (pair[0], profile + totals)


def percentage(numerator, denominator):
    if denominator <= 0:
        return ""
    return format((Decimal(100) * numerator / denominator).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")


def portfolio_row(pair):
    name, team, role, assigned, purchasing, repeat, gross, cancellations, adjustments, largest = pair[1]
    per_customer = ""
    if assigned > 0:
        per_customer = format((gross / assigned).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")
    return (pair[0], name, team, role, assigned, purchasing, repeat, f"{gross:.2f}",
            f"{cancellations:.2f}", f"{adjustments:.2f}", f"{gross - cancellations - adjustments:.2f}",
            per_customer, percentage(repeat, purchasing), percentage(largest, gross))


def run_task_4(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD, output_dir):
    periodTransactionsRDD = classifiedTransactionsRDD.filter(lambda record: record[0] in FINANCIAL_CLASSES)
    periodTransactionsRDD = periodTransactionsRDD.filter(
        lambda record: ANALYSIS_START <= record[3][4] < ANALYSIS_END
    )
    customerAmountsRDD = periodTransactionsRDD.map(customer_amounts)
    # key=CustomerID (empty for anonymous rows), value=(GrossSales, Cancellations, Adjustments)
    customerTotalsRDD = customerAmountsRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    ).cache()
    # key=CustomerID, value=the three summed amounts

    # Request 1: count all assigned profiles, purchasing customers and repeat customers.
    knownPurchasesRDD = periodTransactionsRDD.filter(lambda record: record[0] == "Sale" and record[3][6] != "")
    customerInvoicesRDD = knownPurchasesRDD.map(lambda record: (record[3][6], record[3][0])).distinct()
    # key=CustomerID, value=distinct purchase InvoiceNo
    invoiceUnitsRDD = customerInvoicesRDD.map(lambda pair: (pair[0], 1))
    # key=CustomerID, value=1 per distinct purchase invoice
    invoiceCountsRDD = invoiceUnitsRDD.reduceByKey(lambda a, b: a + b)
    # key=CustomerID, value=PurchaseInvoices
    customerKeysRDD = customersRDD.map(lambda row: (row[0], 1))
    # key=CustomerID, value=1 for every customer profile, including inactive customers
    profilesWithAmountsRDD = customerKeysRDD.leftOuterJoin(customerTotalsRDD)
    # key=CustomerID, value=(1, amounts or None)
    allCustomerAmountsRDD = profilesWithAmountsRDD.map(
        lambda pair: (pair[0], pair[1][1] if pair[1][1] is not None else (Decimal("0"), Decimal("0"), Decimal("0")))
    )
    # key=CustomerID, value=(GrossSales, Cancellations, Adjustments), with zero defaults
    customerActivityRDD = allCustomerAmountsRDD.leftOuterJoin(invoiceCountsRDD)
    # key=CustomerID, value=((GrossSales, Cancellations, Adjustments), PurchaseInvoices or None)
    customerMetricsRDD = customerActivityRDD.map(customer_flags)
    # key=CustomerID, value=(1 assigned profile, purchasing flag, repeat flag, GrossSales, Cancellations, Adjustments, customer GrossSales)
    assignmentsByCustomerRDD = assignmentsRDD.map(lambda row: (row[0], row[1]))
    # key=CustomerID, value=EmployeeID
    assignedCustomersRDD = assignmentsByCustomerRDD.join(customerMetricsRDD)
    # key=CustomerID, value=(EmployeeID, customer metrics)
    customersByEmployeeRDD = assignedCustomersRDD.map(lambda pair: (pair[1][0], pair[1][1]))
    # key=EmployeeID, value=customer metrics
    employeeTotalsRDD = customersByEmployeeRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3],
                      a[4] + b[4], a[5] + b[5], max(a[6], b[6]))
    )
    # key=EmployeeID, value=(AssignedCustomers, PurchasingCustomers, RepeatCustomers, GrossSales, Cancellations, Adjustments, largest customer GrossSales)
    employeeProfilesRDD = employeesRDD.map(lambda row: (row[0], (row[1], row[2], row[3])))
    # key=EmployeeID, value=(EmployeeName, Team, Role)
    employeesWithTotalsRDD = employeeProfilesRDD.leftOuterJoin(employeeTotalsRDD)
    # key=EmployeeID, value=((EmployeeName, Team, Role), portfolio metrics or None)
    employeeResultsRDD = employeesWithTotalsRDD.map(employee_metrics).cache()
    # key=EmployeeID, value=(EmployeeName, Team, Role, AssignedCustomers, PurchasingCustomers, RepeatCustomers, GrossSales, Cancellations, Adjustments, largest customer GrossSales)
    try:
        portfolioRowsRDD = employeeResultsRDD.map(portfolio_row)
        # row=(EmployeeID, EmployeeName, Team, Role, AssignedCustomers, PurchasingCustomers, RepeatCustomers, GrossSales, Cancellations, Adjustments, NetSales, GrossPerAssignedCustomer, RepeatSharePct, LargestCustomerSharePct)
        write_csv(portfolioRowsRDD, output_dir / "task_4" / "employee_portfolios", (
            "EmployeeID", "EmployeeName", "Team", "Role", "AssignedCustomers", "PurchasingCustomers",
            "RepeatCustomers", "GrossSales", "Cancellations", "Adjustments", "NetSales",
            "GrossPerAssignedCustomer", "RepeatSharePct", "LargestCustomerSharePct",
        ))

        # Request 2: preserve every employee tied for the highest NetSales in their team.
        employeesByTeamRDD = employeeResultsRDD.map(lambda pair: (
            pair[1][1], (pair[0], pair[1][0], pair[1][6] - pair[1][7] - pair[1][8])
        ))
        # key=Team, value=(EmployeeID, EmployeeName, NetSales)
        teamAmountsRDD = employeesByTeamRDD.map(lambda pair: (pair[0], pair[1][2]))
        # key=Team, value=one employee's NetSales
        teamMaxRDD = teamAmountsRDD.reduceByKey(lambda a, b: max(a, b))
        # key=Team, value=maximum NetSales
        employeesWithMaxRDD = employeesByTeamRDD.join(teamMaxRDD)
        # key=Team, value=((EmployeeID, EmployeeName, NetSales), maximum NetSales)
        leadersRDD = employeesWithMaxRDD.filter(lambda pair: pair[1][0][2] == pair[1][1])
        leaderRowsRDD = leadersRDD.map(lambda pair: (pair[0], pair[1][0][0], pair[1][0][1], f"{pair[1][0][2]:.2f}"))
        # row=(Team, EmployeeID, EmployeeName, NetSales)
        write_csv(leaderRowsRDD, output_dir / "task_4" / "team_leaders", (
            "Team", "EmployeeID", "EmployeeName", "NetSales",
        ))

        # Request 3: reconcile actual employee totals, unassigned customers and anonymous rows.
        zero = (Decimal("0"), Decimal("0"), Decimal("0"))
        overallAmountsRDD = customerTotalsRDD.map(lambda pair: pair[1])
        # value=(GrossSales, Cancellations, Adjustments), for every customer/anonymous group
        # fold sums the triples; the zero tuple also handles an empty RDD.
        overall = overallAmountsRDD.fold(zero, lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
        assignedAmountsRDD = employeeResultsRDD.map(lambda pair: (pair[1][6], pair[1][7], pair[1][8]))
        # value=(GrossSales, Cancellations, Adjustments), for one employee
        assigned = assignedAmountsRDD.fold(zero, lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
        customerAttributionRDD = customerTotalsRDD.leftOuterJoin(assignmentsByCustomerRDD)
        # key=CustomerID, value=((GrossSales, Cancellations, Adjustments), EmployeeID or None)
        unattributedRDD = customerAttributionRDD.filter(lambda pair: pair[1][1] is None)
        unattributedAmountsRDD = unattributedRDD.map(
            lambda pair: ("Anonymous" if pair[0] == "" else "Unassigned", pair[1][0])
        )
        # key=Anonymous or Unassigned, value=(GrossSales, Cancellations, Adjustments)
        unattributedTotalsRDD = unattributedAmountsRDD.reduceByKey(
            lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
        )
        # key=Anonymous or Unassigned, value=the three summed amounts
        unattributed = unattributedTotalsRDD.collectAsMap()
        # Only these two aggregate groups are returned to Python.
        unassigned = unattributed.get("Unassigned", zero)
        anonymous = unattributed.get("Anonymous", zero)
        # Each total now contains (GrossSales, Cancellations, Adjustments, NetSales).
        totals = [(g, c, a, g - c - a) for g, c, a in (overall, assigned, unassigned, anonymous)]
        reconciliation = []
        for index, metric in enumerate(("GrossSales", "Cancellations", "Adjustments", "NetSales")):
            amounts = [values[index] for values in totals]
            difference = amounts[0] - amounts[1] - amounts[2] - amounts[3]
            if difference != 0:
                raise AssertionError(f"Sales reconciliation failed for {metric}: {difference}")
            reconciliation.append((metric, f"{amounts[0]:.2f}", f"{amounts[1]:.2f}",
                                   f"{amounts[2]:.2f}", f"{amounts[3]:.2f}", f"{difference:.2f}"))
        reconciliationRDD = classifiedTransactionsRDD.context.parallelize(reconciliation, 1)
        # row=(Metric, OverallTotal, AssignedTotal, UnassignedTotal, AnonymousTotal, Difference)
        write_csv(reconciliationRDD, output_dir / "task_4" / "sales_reconciliation", (
            "Metric", "OverallTotal", "AssignedTotal", "UnassignedTotal", "AnonymousTotal", "Difference",
        ))
    finally:
        customerTotalsRDD.unpersist()
        employeeResultsRDD.unpersist()
