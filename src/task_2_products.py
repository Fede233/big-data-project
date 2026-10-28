"""Task 2: product results, cancellation watchlist and country leaders."""

from decimal import Decimal, ROUND_HALF_UP

from common import ANALYSIS_START, ANALYSIS_END, write_csv
from quality import FINANCIAL_CLASSES


def line_amount(record):
    transaction = record[3]
    return (transaction[3] * transaction[5]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def product_amounts(record):
    amount = line_amount(record)
    units = 0
    gross = cancellations = adjustments = Decimal("0")
    if record[0] == "Sale":
        units = record[3][3]
        gross = amount
    elif record[0] == "Cancellation":
        cancellations = abs(amount)
    else:
        adjustments = abs(amount)
    return (record[3][1], (units, gross, cancellations, adjustments))


def product_counts(pair):
    (amounts, invoices), buyers = pair[1]
    return (pair[0], amounts + (invoices or 0, buyers or 0))


def product_row(pair):
    units, gross, cancellations, adjustments, invoices, buyers = pair[1]
    percentage = ""
    if gross > 0:
        percentage = format((100 * cancellations / gross).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")
    return (pair[0], units, f"{gross:.2f}", f"{cancellations:.2f}", f"{adjustments:.2f}",
            f"{gross - cancellations - adjustments:.2f}", percentage, invoices, buyers)


def run_task_2(classifiedTransactionsRDD, output_dir):
    periodTransactionsRDD = classifiedTransactionsRDD.filter(lambda record: record[0] in FINANCIAL_CLASSES)
    periodTransactionsRDD = periodTransactionsRDD.filter(
        lambda record: ANALYSIS_START <= record[3][4] < ANALYSIS_END
    )
    purchasesRDD = periodTransactionsRDD.filter(lambda record: record[0] == "Sale")
    knownPurchasesRDD = purchasesRDD.filter(lambda record: record[3][6] != "")

    # Request 1: product amounts, distinct purchase invoices and identified buyers.
    productAmountsRDD = periodTransactionsRDD.map(product_amounts)
    # key=StockCode, value=(UnitsSold, GrossSales, Cancellations, Adjustments)
    productTotalsRDD = productAmountsRDD.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])
    )
    # key=StockCode, value=the four summed metrics
    productInvoicesRDD = purchasesRDD.map(lambda record: (record[3][1], record[3][0])).distinct()
    # key=StockCode, value=distinct purchase InvoiceNo
    invoiceUnitsRDD = productInvoicesRDD.map(lambda pair: (pair[0], 1))
    # key=StockCode, value=1 per distinct purchase invoice
    invoiceCountsRDD = invoiceUnitsRDD.reduceByKey(lambda a, b: a + b)
    # key=StockCode, value=PurchaseInvoices
    productBuyersRDD = knownPurchasesRDD.map(lambda record: (record[3][1], record[3][6])).distinct()
    # key=StockCode, value=distinct identified CustomerID
    buyerUnitsRDD = productBuyersRDD.map(lambda pair: (pair[0], 1))
    # key=StockCode, value=1 per distinct buyer
    buyerCountsRDD = buyerUnitsRDD.reduceByKey(lambda a, b: a + b)
    # key=StockCode, value=Buyers
    productsWithInvoicesRDD = productTotalsRDD.leftOuterJoin(invoiceCountsRDD)
    # key=StockCode, value=((UnitsSold, GrossSales, Cancellations, Adjustments), PurchaseInvoices or None)
    productsWithBuyersRDD = productsWithInvoicesRDD.leftOuterJoin(buyerCountsRDD)
    # key=StockCode, value=(((UnitsSold, GrossSales, Cancellations, Adjustments), PurchaseInvoices or None), Buyers or None)
    productResultsRDD = productsWithBuyersRDD.map(product_counts).cache()
    # key=StockCode, value=(UnitsSold, GrossSales, Cancellations, Adjustments, PurchaseInvoices, Buyers)
    columns = ("StockCode", "UnitsSold", "GrossSales", "Cancellations", "Adjustments",
               "NetSales", "CancellationPct", "PurchaseInvoices", "Buyers")
    try:
        productRowsRDD = productResultsRDD.map(product_row)
        # row=(StockCode, UnitsSold, GrossSales, Cancellations, Adjustments, NetSales, CancellationPct, PurchaseInvoices, Buyers)
        write_csv(productRowsRDD, output_dir / "task_2" / "product_summary", columns)

        # Request 2: apply the thresholds before formatting percentages.
        watchlistRDD = productResultsRDD.filter(
            lambda pair: pair[1][4] >= 10 and pair[1][1] > 0 and 10 * pair[1][2] >= pair[1][1]
        )
        # Same key/value; Cancellations / GrossSales >= 0.10 without display rounding.
        watchlistRowsRDD = watchlistRDD.map(product_row)
        # row=the same nine columns as product_summary
        write_csv(watchlistRowsRDD, output_dir / "task_2" / "product_watchlist", columns)

        # Request 3: find every product tied for maximum gross sales in each country.
        countryProductAmountsRDD = purchasesRDD.map(
            lambda record: ((record[3][7], record[3][1]), line_amount(record))
        )
        # key=(Country, StockCode), value=Sale line amount, including anonymous sales
        countryProductTotalsRDD = countryProductAmountsRDD.reduceByKey(lambda a, b: a + b)
        # key=(Country, StockCode), value=GrossSales
        productsByCountryRDD = countryProductTotalsRDD.map(lambda pair: (pair[0][0], (pair[0][1], pair[1])))
        # key=Country, value=(StockCode, GrossSales)
        countryAmountsRDD = productsByCountryRDD.map(lambda pair: (pair[0], pair[1][1]))
        # key=Country, value=one product's GrossSales
        countryMaxRDD = countryAmountsRDD.reduceByKey(lambda a, b: max(a, b))
        # key=Country, value=maximum GrossSales
        productsWithMaxRDD = productsByCountryRDD.join(countryMaxRDD)
        # key=Country, value=((StockCode, GrossSales), maximum GrossSales)
        leadersRDD = productsWithMaxRDD.filter(lambda pair: pair[1][0][1] == pair[1][1])
        leadersByProductRDD = leadersRDD.map(lambda pair: ((pair[0], pair[1][0][0]), pair[1][0][1]))
        # key=(Country, StockCode), value=GrossSales of a tied winner
        countryProductBuyersRDD = knownPurchasesRDD.map(
            lambda record: ((record[3][7], record[3][1]), record[3][6])
        ).distinct()
        # key=(Country, StockCode), value=distinct identified CustomerID
        countryBuyerUnitsRDD = countryProductBuyersRDD.map(lambda pair: (pair[0], 1))
        # key=(Country, StockCode), value=1 per distinct buyer
        countryBuyerCountsRDD = countryBuyerUnitsRDD.reduceByKey(lambda a, b: a + b)
        # key=(Country, StockCode), value=Buyers
        leadersWithBuyersRDD = leadersByProductRDD.leftOuterJoin(countryBuyerCountsRDD)
        # key=(Country, StockCode), value=(GrossSales, Buyers or None)
        leaderRowsRDD = leadersWithBuyersRDD.map(
            lambda pair: (pair[0][0], pair[0][1], f"{pair[1][0]:.2f}", pair[1][1] or 0)
        )
        # row=(Country, StockCode, GrossSales, Buyers)
        write_csv(leaderRowsRDD, output_dir / "task_2" / "country_product_leaders", (
            "Country", "StockCode", "GrossSales", "Buyers",
        ))
    finally:
        productResultsRDD.unpersist()
