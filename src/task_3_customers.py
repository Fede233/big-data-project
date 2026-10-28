"""Task 3: customer purchasing history and service-segment results."""

from decimal import Decimal, ROUND_HALF_UP

from common import ANALYSIS_START, ANALYSIS_END, write_csv
from quality import FINANCIAL_CLASSES


def customer_amounts(record):
    transaction = record[3]
    amount = (transaction[3] * transaction[5]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    gross = cancellations = adjustments = Decimal("0")
    if record[0] == "Sale":
        gross = amount
    elif record[0] == "Cancellation":
        cancellations = abs(amount)
    else:
        adjustments = abs(amount)
    return (transaction[6], (gross, cancellations, adjustments))


def customer_metrics(pair):
    (profile, amounts), activity = pair[1]
    if amounts is None:
        amounts = (Decimal("0"), Decimal("0"), Decimal("0"))
    if activity is None:
        activity = (0, None, None)
    segment, plan = profile
    gross, cancellations, adjustments = amounts
    invoices, first, last = activity
    return (pair[0], (segment, plan, invoices, gross, cancellations, adjustments, first, last))


def customer_row(pair):
    segment, plan, invoices, gross, cancellations, adjustments, first, last = pair[1]
    first_date = first.strftime("%Y-%m-%d %H:%M:%S") if first is not None else ""
    last_date = last.strftime("%Y-%m-%d %H:%M:%S") if last is not None else ""
    return (pair[0], segment, plan, invoices, f"{gross:.2f}", f"{cancellations:.2f}",
            f"{adjustments:.2f}", f"{gross - cancellations - adjustments:.2f}",
            first_date, last_date, "true" if invoices >= 2 else "false")


def segment_row(pair):
    customers, purchasing, repeat, gross, net = pair[1]
    percentage = ""
    if purchasing > 0:
        percentage = format((Decimal(100) * repeat / purchasing).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")
    return (pair[0][0], pair[0][1], customers, purchasing, repeat, percentage, f"{gross:.2f}", f"{net:.2f}")


def run_task_3(classifiedTransactionsRDD, customersRDD, output_dir):
    periodTransactionsRDD = classifiedTransactionsRDD.filter(lambda record: record[0] in FINANCIAL_CLASSES)
    periodTransactionsRDD = periodTransactionsRDD.filter(
        lambda record: ANALYSIS_START <= record[3][4] < ANALYSIS_END and record[3][6] != ""
    )

    # Request 1: monetary results and purchase activity for every customer.
    customerAmountsRDD = periodTransactionsRDD.map(customer_amounts)
    # key=CustomerID, value=(GrossSales, Cancellations, Adjustments)
    customerTotalsRDD = customerAmountsRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )
    # key=CustomerID, value=the three summed amounts
    purchasesRDD = periodTransactionsRDD.filter(lambda record: record[0] == "Sale")
    invoiceDatesRDD = purchasesRDD.map(
        lambda record: ((record[3][6], record[3][0]), (record[3][4], record[3][4]))
    )
    # key=(CustomerID, InvoiceNo), value=(first line date, last line date)
    distinctInvoicesRDD = invoiceDatesRDD.reduceByKey(
        lambda a, b: (min(a[0], b[0]), max(a[1], b[1]))
    )
    # key=(CustomerID, InvoiceNo), value=(first Sale date, last Sale date) for one distinct invoice
    customerInvoicesRDD = distinctInvoicesRDD.map(lambda pair: (pair[0][0], (1, pair[1][0], pair[1][1])))
    # key=CustomerID, value=(1 purchase invoice, first Sale date, last Sale date)
    customerActivityRDD = customerInvoicesRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], min(a[1], b[1]), max(a[2], b[2]))
    )
    # key=CustomerID, value=(PurchaseInvoices, FirstPurchaseDate, LastPurchaseDate)
    profilesRDD = customersRDD.map(lambda row: (row[0], (row[1], row[2])))
    # key=CustomerID, value=(Segment, ServicePlan)
    profilesWithAmountsRDD = profilesRDD.leftOuterJoin(customerTotalsRDD)
    # key=CustomerID, value=((Segment, ServicePlan), (GrossSales, Cancellations, Adjustments) or None)
    profilesWithActivityRDD = profilesWithAmountsRDD.leftOuterJoin(customerActivityRDD)
    # key=CustomerID, value=(((Segment, ServicePlan), amounts or None), purchase activity or None)
    customerResultsRDD = profilesWithActivityRDD.map(customer_metrics).cache()
    # key=CustomerID, value=(Segment, ServicePlan, PurchaseInvoices, GrossSales, Cancellations, Adjustments, FirstPurchaseDate, LastPurchaseDate)
    try:
        customerRowsRDD = customerResultsRDD.map(customer_row)
        # row=(CustomerID, Segment, ServicePlan, PurchaseInvoices, GrossSales, Cancellations, Adjustments, NetSales, FirstPurchaseDate, LastPurchaseDate, IsRepeat)
        write_csv(customerRowsRDD, output_dir / "task_3" / "customer_summary", (
            "CustomerID", "Segment", "ServicePlan", "PurchaseInvoices", "GrossSales", "Cancellations",
            "Adjustments", "NetSales", "FirstPurchaseDate", "LastPurchaseDate", "IsRepeat",
        ))

        # Request 2: compare all service profiles, including those without purchases.
        segmentCustomersRDD = customerResultsRDD.map(lambda pair: (
            (pair[1][0], pair[1][1]),
            (1, 1 if pair[1][2] > 0 else 0, 1 if pair[1][2] >= 2 else 0,
             pair[1][3], pair[1][3] - pair[1][4] - pair[1][5])
        ))
        # key=(Segment, ServicePlan), value=(1 customer, purchasing flag, repeat flag, GrossSales, NetSales)
        segmentTotalsRDD = segmentCustomersRDD.reduceByKey(
            lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3], a[4] + b[4])
        )
        # key=(Segment, ServicePlan), value=(Customers, PurchasingCustomers, RepeatCustomers, GrossSales, NetSales)
        segmentRowsRDD = segmentTotalsRDD.map(segment_row)
        # row=(Segment, ServicePlan, Customers, PurchasingCustomers, RepeatCustomers, RepeatSharePct, GrossSales, NetSales)
        write_csv(segmentRowsRDD, output_dir / "task_3" / "customer_segments", (
            "Segment", "ServicePlan", "Customers", "PurchasingCustomers", "RepeatCustomers",
            "RepeatSharePct", "GrossSales", "NetSales",
        ))
    finally:
        customerResultsRDD.unpersist()
