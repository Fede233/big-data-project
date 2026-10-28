"""Task 5: monthly employee bonuses under the specified policy."""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from common import ANALYSIS_START, ANALYSIS_END, write_csv
from quality import FINANCIAL_CLASSES
from task_3_customers import customer_amounts


def bonus_row(pair):
    # Apply the policy in order and format one employee/month result.
    employee, month = pair[0]
    amounts = pair[1][1]
    if amounts is None:
        amounts = (Decimal("0"), Decimal("0"), Decimal("0"))
    gross, cancellations, adjustments = amounts
    net = gross - cancellations - adjustments
    bonus = Decimal("0")
    if gross == 0:
        status = "NO_SALES"
    elif net < Decimal("10000"):
        status = "TARGET_NOT_MET"
    elif 10 * cancellations > gross:
        # Strictly above 10%; do not use the rounded display percentage.
        status = "HIGH_CANCELLATIONS"
    else:
        status = "ELIGIBLE"
        bonus = min(Decimal("0.01") * net, Decimal("500"))
    bonus = bonus.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    percentage = ""
    if gross > 0:
        percentage = format((100 * cancellations / gross).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")
    return (employee, month, f"{gross:.2f}", f"{cancellations:.2f}", f"{adjustments:.2f}",
            f"{net:.2f}", percentage, status, f"{bonus:.2f}")


def run_task_5(classifiedTransactionsRDD, employeesRDD, assignmentsRDD, output_dir):
    periodTransactionsRDD = classifiedTransactionsRDD.filter(lambda record: record[0] in FINANCIAL_CLASSES)
    periodTransactionsRDD = periodTransactionsRDD.filter(
        lambda record: ANALYSIS_START <= record[3][4] < ANALYSIS_END and record[3][6] != ""
    )
    customerMonthAmountsRDD = periodTransactionsRDD.map(lambda record: (
        record[3][6], (record[3][4].strftime("%Y-%m"), customer_amounts(record)[1])
    ))
    # key=CustomerID, value=(Month, (GrossSales, Cancellations, Adjustments) for one line)
    assignmentsByCustomerRDD = assignmentsRDD.map(lambda row: (row[0], row[1]))
    # key=CustomerID, value=EmployeeID
    assignedAmountsRDD = customerMonthAmountsRDD.join(assignmentsByCustomerRDD)
    # key=CustomerID, value=((Month, line amounts), EmployeeID)
    employeeMonthAmountsRDD = assignedAmountsRDD.map(
        lambda pair: ((pair[1][1], pair[1][0][0]), pair[1][0][1])
    )
    # key=(EmployeeID, Month), value=(GrossSales, Cancellations, Adjustments) for one line
    employeeMonthTotalsRDD = employeeMonthAmountsRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )
    # key=(EmployeeID, Month), value=the three summed amounts

    months = []
    for year in range(ANALYSIS_START.year, ANALYSIS_END.year + 1):
        for month in range(1, 13):
            if ANALYSIS_START <= datetime(year, month, 1) < ANALYSIS_END:
                months.append(f"{year}-{month:02d}")
    employeeMonthsRDD = employeesRDD.flatMap(lambda row: [((row[0], month), 1) for month in months])
    # key=(EmployeeID, Month), value=1 for every employee/month, including inactive ones
    completeMonthsRDD = employeeMonthsRDD.leftOuterJoin(employeeMonthTotalsRDD)
    # key=(EmployeeID, Month), value=(1, (GrossSales, Cancellations, Adjustments) or None)
    bonusRowsRDD = completeMonthsRDD.map(bonus_row)
    # row=(EmployeeID, Month, GrossSales, Cancellations, Adjustments, NetSales, CancellationPct, BonusStatus, BonusAmount)
    write_csv(bonusRowsRDD, output_dir / "task_5" / "employee_monthly_bonus", (
        "EmployeeID", "Month", "GrossSales", "Cancellations", "Adjustments", "NetSales",
        "CancellationPct", "BonusStatus", "BonusAmount",
    ))
