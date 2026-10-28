"""Task 1: monthly sales and comparable country growth."""

from decimal import Decimal, ROUND_HALF_UP

from common import ANALYSIS_START, ANALYSIS_END, write_csv
from quality import FINANCIAL_CLASSES


def monthly_amounts(record):
    transaction = record[3]
    amount = (transaction[3] * transaction[5]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    gross = cancellations = adjustments = anonymous = Decimal("0")
    if record[0] == "Sale":
        gross = amount
        if transaction[6] == "":
            anonymous = amount
    elif record[0] == "Cancellation":
        cancellations = abs(amount)
    else:
        adjustments = abs(amount)
    return ((transaction[7], transaction[4].strftime("%Y-%m")), (gross, cancellations, adjustments, anonymous))


def monthly_row(pair):
    country, month = pair[0]
    amounts, invoices = pair[1]
    gross, cancellations, adjustments, anonymous = amounts
    percentage = ""
    if gross > 0:
        percentage = format((100 * cancellations / gross).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")
    return (country, month, f"{gross:.2f}", f"{cancellations:.2f}", f"{adjustments:.2f}",
            f"{gross - cancellations - adjustments:.2f}", percentage, invoices or 0, f"{anonymous:.2f}")


def growth_row(pair):
    country = pair[0]
    previous, current = pair[1]
    change = current - previous
    percentage = ""
    if previous > 0:
        percentage = format((100 * change / previous).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")
    return (country, f"{previous:.2f}", f"{current:.2f}", f"{change:.2f}", percentage)


def run_task_1(classifiedTransactionsRDD, output_dir):
    periodTransactionsRDD = classifiedTransactionsRDD.filter(lambda record: record[0] in FINANCIAL_CLASSES)
    periodTransactionsRDD = periodTransactionsRDD.filter(
        lambda record: ANALYSIS_START <= record[3][4] < ANALYSIS_END
    )

    # Request 1: monthly results by country.
    monthlyAmountsRDD = periodTransactionsRDD.map(monthly_amounts)
    # key=(Country, Month), value=(GrossSales, Cancellations, Adjustments, AnonymousGrossSales)
    monthlyTotalsRDD = monthlyAmountsRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])
    ).cache()
    # key=(Country, Month), value=the four summed amounts

    purchasesRDD = periodTransactionsRDD.filter(lambda record: record[0] == "Sale")
    purchaseInvoicesRDD = purchasesRDD.map(
        lambda record: ((record[3][7], record[3][4].strftime("%Y-%m")), record[3][0])
    ).distinct()
    # key=(Country, Month), value=distinct purchase InvoiceNo
    invoiceUnitsRDD = purchaseInvoicesRDD.map(lambda pair: (pair[0], 1))
    # key=(Country, Month), value=1 per distinct purchase invoice
    invoiceCountsRDD = invoiceUnitsRDD.reduceByKey(lambda a, b: a + b)
    # key=(Country, Month), value=PurchaseInvoices
    monthlyJoinedRDD = monthlyTotalsRDD.leftOuterJoin(invoiceCountsRDD)
    # key=(Country, Month), value=((GrossSales, Cancellations, Adjustments, AnonymousGrossSales), PurchaseInvoices or None)
    monthlyResultsRDD = monthlyJoinedRDD.map(monthly_row)
    # row=(Country, Month, GrossSales, Cancellations, Adjustments, NetSales, CancellationPct, PurchaseInvoices, AnonymousGrossSales)
    try:
        write_csv(monthlyResultsRDD, output_dir / "task_1" / "country_monthly", (
            "Country", "Month", "GrossSales", "Cancellations", "Adjustments", "NetSales",
            "CancellationPct", "PurchaseInvoices", "AnonymousGrossSales",
        ))

        # Request 2: January-November in each year, excluding December 2010.
        comparableMonthsRDD = monthlyTotalsRDD.filter(lambda pair: pair[0][1][5:7] != "12")
        countryYearAmountsRDD = comparableMonthsRDD.map(
            lambda pair: ((pair[0][0], pair[0][1][:4]), pair[1][0] - pair[1][1] - pair[1][2])
        )
        # key=(Country, Year), value=monthly NetSales
        countryYearNetRDD = countryYearAmountsRDD.reduceByKey(lambda a, b: a + b)
        # key=(Country, Year), value=NetSales for January-November
        countryYearsRDD = countryYearNetRDD.map(lambda pair: (
            pair[0][0], (pair[1] if pair[0][1] == "2010" else Decimal("0"),
                         pair[1] if pair[0][1] == "2011" else Decimal("0"))
        ))
        # key=Country, value=(NetSales2010 or 0, NetSales2011 or 0)
        countryComparisonRDD = countryYearsRDD.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        # key=Country, value=(NetSales2010, NetSales2011), including countries present in only one year
        growthResultsRDD = countryComparisonRDD.map(growth_row)
        # row=(Country, NetSales2010, NetSales2011, NetChange, GrowthPct)
        write_csv(growthResultsRDD, output_dir / "task_1" / "country_growth", (
            "Country", "NetSales2010", "NetSales2011", "NetChange", "GrowthPct",
        ))
    finally:
        monthlyTotalsRDD.unpersist()
