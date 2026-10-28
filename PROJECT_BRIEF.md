# Retail Sales & Customer Operations Analytics

## Project Brief

## 1. Business scenario and objective

PoliRetail is a fictional UK-based online retailer serving domestic and
international customers. Its commercial managers want to understand sales,
cancellations, customer purchasing behaviour and the portfolios managed by
their account managers.

The project combines real transaction records with simulated customer profiles,
employees and customer assignments.

Implement Tasks 1-5 using the four inputs below and the supplied Spark runner.
Write only the five task modules listed in Section 6.1 and `docs/results.md`.
All other files are provided; use them without modifying them.

## 2. Input files

All analytical inputs are UTF-8, comma-separated files with a header. Empty
fields represent missing values. Text fields may contain quoted commas;
the supplied preparer replaces embedded line breaks with spaces so each record
occupies one physical line. Dates use `YYYY-MM-DD HH:MM:SS`; identifiers are
strings.

The four prepared files are stored in `data/inputs/`; the source workbook is
stored in `data/raw/`. Acquisition details and input checksums are recorded in
`data/input_manifest.json`.

### 2.1 `transactions.csv` - real invoice lines

This file contains invoice lines from Online Retail II, published by the
UCI Machine Learning Repository, covering December 2009 to December 2011.
The supplied preparer converts the source workbook. The workbook columns
`Invoice`, `Price` and `Customer ID` correspond to `InvoiceNo`, `UnitPrice` and
`CustomerID` below; other column names are unchanged. Each row represents one
item recorded on an invoice:

| Field | Type | Meaning |
| --- | --- | --- |
| InvoiceNo | string | Invoice identifier; a leading `C` denotes cancellation. |
| StockCode | string | Product or item code, not necessarily a physical good. |
| Description | string, optional | Source item description; it may vary for the same code. |
| Quantity | integer | Signed quantity recorded on the invoice line. |
| InvoiceDate | timestamp | Source invoice date and time, without timezone conversion. |
| UnitPrice | decimal | Unit price in GBP. |
| CustomerID | string, optional | Source customer identifier. |
| Country | string, optional | Country recorded on the transaction. |

An invoice may contain several lines. Customer identifiers are stored without
an Excel-style `.0` suffix.

The source sheets overlap in December 2010: 22,523 records in the first sheet
also occur in the second. The supplied preparer uses the first sheet
before `2010-12-01` and the second sheet from that date onwards. This produces
1,044,848 input rows from 1,067,371 workbook records. Duplicates within each
retained sheet period are preserved; overlap removal is recorded in the manifest.

### 2.2 `customers.csv` - simulated customer profiles

Each row describes one customer and the service profile used by the commercial
team:

| Field | Type | Meaning |
| --- | --- | --- |
| CustomerID | string | Unique identifier taken from the source transactions. |
| Segment | string | Simulated classification: `Retail` or `Wholesale`. |
| ServicePlan | string | Simulated service level: `Standard` or `Priority`. |

The file includes every distinct, non-empty CustomerID from parseable source records,
even without purchases in the analysis period. Transactions without CustomerID
do not create a customer profile.

### 2.3 `employees.csv` - simulated commercial staff

Each row describes one employee responsible for a customer portfolio:

| Field | Type | Meaning |
| --- | --- | --- |
| EmployeeID | string | Unique identifier, `E001` to `E012`. |
| EmployeeName | string | Fictional display name. |
| Team | string | `TeamA`, `TeamB` or `TeamC`, with four employees per team. |
| Role | string | `AccountManager` or `SeniorAccountManager`. |

All employees belong to the simulation for the entire analysis period. Role is
descriptive; the bonus policy in Task 5 applies equally to both roles.

### 2.4 `customer_assignments.csv` - simulated portfolios

Each row assigns a customer to an employee:

| Field | Type | Meaning |
| --- | --- | --- |
| CustomerID | string | A customer present in `customers.csv`. |
| EmployeeID | string | The customer's designated commercial contact. |

Each customer has at most one fixed assignment; employees may have many
customers. Unassigned customers have no row. Both foreign keys must exist.

The supplied preparer uses seed 42 and sorted customer IDs. Segment is
70% Retail / 30% Wholesale; independently, ServicePlan is 75% Standard /
25% Priority. It leaves 5% of customers unassigned (rounded down, minimum one),
selects them uniformly and assigns the others uniformly to E001-E011,
independently of sales. E012 has no clients. The first employee in each team
is SeniorAccountManager; the other three are AccountManager.

## 3. Common rules and metric definitions

### 3.1 Record quality and financial classification

A parseable financial record requires non-empty InvoiceNo and StockCode, a
valid InvoiceDate, an integer Quantity and a finite numeric UnitPrice. Description
and CustomerID may be missing. The supplied classifier trims identifier
whitespace, normalises cancellation prefixes and replaces missing Country
with `UNKNOWN`. It assigns each source row to one class:

| Class | Condition | Treatment |
| --- | --- | --- |
| Invalid | A required field is missing or cannot be parsed. | Exclude from financial analyses. |
| Excluded | Parseable, but UnitPrice <= 0, Quantity = 0, or InvoiceNo starts with C while Quantity > 0. | Exclude; report the reason. |
| Sale | UnitPrice > 0, Quantity > 0, non-C invoice. | Positive sales amount. |
| Cancellation | UnitPrice > 0, Quantity < 0, C invoice. | Cancellation amount. |
| Other negative adjustment | UnitPrice > 0, Quantity < 0, non-C invoice. | Separate adjustment amount. |

The supplied quality checks apply exclusion reasons in the listed order,
report duplicate-row excess (`row count - distinct normalised row count`) and
invoices with conflicting non-empty customer IDs or countries. Identical rows
are retained because there is no line identifier. Invalid dimension keys and
foreign keys stop execution with an error.

### 3.2 Period, amounts and ratios

- The runner loads and classifies the entire prepared transaction file. Tasks 1-5 use the months
  from 2010-01-01 inclusive to 2011-12-01 exclusive. This is a business
  filter, not a sample; December 2011 is incomplete.
- For each included line, its signed amount is Quantity x UnitPrice, rounded
  to two decimal places using round-half-up. Aggregate these line amounts.
- GrossSales (G): sum of Sale amounts.
- Cancellations (C): sum of absolute Cancellation amounts.
- Adjustments (A): sum of absolute Other negative adjustment amounts.
- NetSales (N): G - C - A.
- CancellationPct: 100 x C / G when G > 0; otherwise empty.
  This compares cancellation amounts recorded in the period with sales recorded
  in the same period, without matching cancellations to original invoices.
  It may exceed 100% when cancellations relate to earlier sales.
- PurchaseInvoices: distinct InvoiceNo values with at least one Sale line
  in the relevant group and period. Buyers: distinct non-empty CustomerID
  values with at least one Sale line.
- A repeat customer has at least two PurchaseInvoices in the period under
  consideration. First/last purchase dates refer to observed purchases only.
- Undefined ratios and absent purchase dates are empty, not zero. Monetary
  totals and counts for groups without activity are zero. Display amounts
  and percentages with two decimals; apply thresholds before display rounding.

Monetary metrics include transactions without CustomerID. Customer and employee
results use identified customers only. NetSales represents recorded sales less
cancellations and adjustments; no product costs are provided.

## 4. Setup and preliminary work

### 4.0 Setup and execution

Use Python 3.12 and Java 21. Configure interpreter and Java paths for your
operating system. Spark is supplied by PySpark, with two local task threads
(`local[2]`); no separate Spark or Hadoop installation is required.

Create a project environment named `.venv` with Python 3.12 using your IDE or
Python environment tools. Select it as the project interpreter and activate
it in the terminal. From the project root, run:

```sh
python -m pip install --no-cache-dir -r requirements.txt
python scripts/prepare_inputs.py --download
```

Here and below, `python` means the interpreter in the active project `.venv`.
Reuse an existing environment. Java must be available to the process; configure
`JAVA_HOME` for your local JDK if necessary.

If the four inputs already exist, skip preparation. The supplied preparer
handles acquisition, conversion and simulated data, recording source details
and checksums in `data/input_manifest.json`. Do not reimplement it.
Use `--overwrite` only to intentionally regenerate inputs.

Working order:

1. Read Sections 1-3, then run the supplied checks and quality pipeline below.
2. Verify the provided setup:

   ```sh
   python -m unittest discover -s tests -p test_quality.py -v
   python src/main.py
   ```

   Expect `Ran 5 tests` / `OK`, then `PASS` and
   `outputs/quality/quality_summary/` with columns `Check,Count`.
3. Complete Tasks 1-5 in order, one request at a time. Run each completed task
   with `python src/main.py --task 1`, replacing `1` with its number.
   Expect `PASS` and the specified output tables. Later modules are not required.
4. After quality and each task, add the results to `docs/results.md` (Section 5).
5. Run all fourteen supplied checks:
   `python -m unittest discover -s tests -v`.

Existing outputs are protected. For a repeat run, choose a fresh root with
`--output-dir outputs/run_2`. Optional `--input-dir` selects a prepared-input
folder. Both paths must remain inside the project.

In VS Code: open the project folder, select the project `.venv` through
Python: Select Interpreter, then use Run and Debug. Choose Section 4: provided
tests, Section 4: run quality pipeline or Task N and press F5. Pipeline/task
runs ask for a new output folder name; read results in the integrated terminal
and saved CSV files.

### 4.1 Provided support and tests

| Component | Role |
| --- | --- |
| `scripts/prepare_inputs.py` | Prepare the four inputs; not a student exercise. |
| `src/main.py` | Start Spark, load inputs, validate quality and run the selected task. |
| `src/common.py` | CSV reading/writing, normalisation and shared definitions. |
| `src/quality.py` | Classify transactions, validate inputs and produce the quality summary. |
| `tests/test_quality.py` | Five provided checks for classification, CSV handling, quality counts, links and output protection. |
| `tests/test_prepare_inputs.py` | Four checks for the supplied preparer. |
| `tests/test_tasks.py` | Five checks for task results, ties, missing activity and thresholds. |
| `.vscode/launch.json` | Editor runs using the project environment. |

These components are ready to use; no preliminary code or tests need to be
written. Expected input counts are 1,044,848 transactions, 5,942 customers,
12 employees and 5,645 assignments. Keep the supplied tests unchanged.

### 4.2 Data passed to the task functions

`classifiedTransactionsRDD` contains records in this form:
`(category, reason, normalised_row, typed_transaction)`.

- Category: `Invalid`, `Excluded`, `Sale`, `Cancellation` or
  `OtherNegativeAdjustment`.
- Reason: the exclusion/error label, or an empty string for included records.
- Normalised row: string fields in transaction-column order.
- Typed transaction: the same eight fields, with integer Quantity, datetime
  InvoiceDate and Decimal UnitPrice. It is `None` for Invalid records.

Financial analyses use only Sale, Cancellation and OtherNegativeAdjustment
records inside the period from Section 3.2. Missing Country is `UNKNOWN` in
typed transactions. Dimension RDDs contain string tuples in input-column order.
The supplied runner performs quality checks before calling each task.

## 5. Results organisation

Save each request as a separate CSV table at its stated path under `outputs/`.
Each directory contains CSV `part-*` files with headers; together they form
one table. `_SUCCESS` is an execution marker, not data. Use a fresh output root
for repeat runs; console previews do not replace saved results.

Write one short report in `docs/results.md`, in English:

- Overview: a short introduction stating the data source, analysis period,
  simulated information and currency.
- Quality: a small table of classification counts and the main quality issues.
- Tasks 1-5: one section per task, with a short objective, selected results
  in a small table or bullet list, output row counts and a 1-2 sentence comment.
  Use the closing sentence of each task below as the focus of the comment.
- Task 4: also include the complete monetary reconciliation table.


## 6. Technical requirements and completion criteria

### 6.1 Code organisation

Write one module per task using the entry points below. Keep all requests
for a task in its module, in brief order. The runner calls these functions;
no changes to the runner or other supplied files are required.

Use these task entry points, called by the provided runner:

| Module | Function and arguments |
| --- | --- |
| `task_1_sales.py` | `run_task_1(classifiedTransactionsRDD, output_dir)` |
| `task_2_products.py` | `run_task_2(classifiedTransactionsRDD, output_dir)` |
| `task_3_customers.py` | `run_task_3(classifiedTransactionsRDD, customersRDD, output_dir)` |
| `task_4_employees.py` | `run_task_4(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD, output_dir)` |
| `task_5_bonus.py` | `run_task_5(classifiedTransactionsRDD, employeesRDD, assignmentsRDD, output_dir)` |

Each function writes the task tables; no return value is required. Dimension
RDDs contain string tuples in input-column order. The classified RDD contains
the records described in Section 4.2; `output_dir` is a `pathlib.Path`.
Tasks may reuse small helpers from earlier modules, but must read the supplied
RDDs rather than depend on previously saved outputs.

Number the requests in code comments as they appear in each task. Use simple
functions and explicit intermediate RDDs, with a key/value comment whenever
the tuple structure changes. Use the RDDs passed by the runner and small helper functions where useful.
Create each task module when starting that task.

Use `common.write_csv(rowsRDD, destination, columns)` to save rows with CSV
headers; it rejects existing destinations. The runner supplies loaded RDDs
and handles Spark setup. Do not duplicate CSV or environment handling.

### 6.2 Completion checklist

- Implement the five task modules using local PySpark RDDs.
- Execute every task on the complete inputs and save all requested tables with
  their specified columns. Preserve tied winners; row ordering is not required.
- Run the fourteen provided tests and check the output counts and monetary
  reconciliation. Task 5 must contain 276 rows: 12 employees x 23 months.
- Complete `docs/results.md` as described in Section 5.

The submitted work consists of the five task modules and `docs/results.md`.
README, brief, input preparation, quality helpers, runner, dependencies, tests
and editor configurations are supplied material and must remain unchanged.

## Tasks

### Task 1 - Sales trends and comparable growth

Situation: management needs to distinguish sales growth from the effect
of cancellations and adjustments.

1. Monthly country results. For every Country/Month with financially
   included activity, calculate the four monetary metrics, CancellationPct,
   PurchaseInvoices and gross sales without CustomerID.

   Output `outputs/task_1/country_monthly/`:
   `Country,Month,GrossSales,Cancellations,Adjustments,NetSales,CancellationPct,PurchaseInvoices,AnonymousGrossSales`.
   Month uses `YYYY-MM`.

2. Comparable year-on-year results. Compare January-November 2010 with
   January-November 2011 for each country appearing in either window. Missing
   country/year totals are zero. Calculate GrowthPct as
   `100 x (NetSales2011 - NetSales2010) / NetSales2010` only when NetSales2010 > 0.

   Output `outputs/task_1/country_growth/`:
   `Country,NetSales2010,NetSales2011,NetChange,GrowthPct`.

Use these results to identify growing markets and countries where cancellations
or adjustments substantially reduce gross sales.

### Task 2 - Product demand and cancellation exposure

Situation: management wants to identify strong-selling products and items
that warrant investigation. StockCode is the analytical identifier; changing
or missing descriptions must not split one code into different products.

1. Product results. For each StockCode with financially included activity,
   calculate monetary metrics, CancellationPct, positive units sold,
   PurchaseInvoices and Buyers. Quantity refers to Sale lines for UnitsSold.

   Output `outputs/task_2/product_summary/`:
   `StockCode,UnitsSold,GrossSales,Cancellations,Adjustments,NetSales,CancellationPct,PurchaseInvoices,Buyers`.

2. Cancellation watchlist. Select products with at least 10
   PurchaseInvoices and CancellationPct >= 10%. Use the same schema in
   `outputs/task_2/product_watchlist/`.

3. Leading products by country. Select every StockCode tied for the highest
   GrossSales in each country with at least one Sale. Include anonymous sales
   in the amount, but not in Buyers.

   Output `outputs/task_2/country_product_leaders/`:
   `Country,StockCode,GrossSales,Buyers`.

Use these results to identify commercially important products and prioritise
items for a review of their cancellation activity.

### Task 3 - Customer recurrence and service segments

Situation: the commercial team needs to understand recurring purchasing
and how activity is distributed across customer segments and service plans.

1. Customer results. Report every customer from `customers.csv`, including
   customers without purchases. Calculate the metrics over the analysis period;
   FirstPurchaseDate and LastPurchaseDate use Sale lines only. IsRepeat is
   `true` when PurchaseInvoices >= 2, otherwise `false`.

   Output `outputs/task_3/customer_summary/`:
   `CustomerID,Segment,ServicePlan,PurchaseInvoices,GrossSales,Cancellations,Adjustments,NetSales,FirstPurchaseDate,LastPurchaseDate,IsRepeat`.

2. Segment comparison. For every Segment/ServicePlan combination present
   in the customer input, count all customers, purchasing customers and repeat
   customers. RepeatSharePct is repeat customers / purchasing customers x 100.
   Include GrossSales and NetSales.

   Output `outputs/task_3/customer_segments/`:
   `Segment,ServicePlan,Customers,PurchasingCustomers,RepeatCustomers,RepeatSharePct,GrossSales,NetSales`.

Use these results to compare customer recurrence and sales across the service
profiles.

### Task 4 - Employee portfolios and concentration

Situation: employees manage portfolios of different sizes. Management
needs to compare their sales, customer recurrence and dependence on individual
customers. Attribute a known customer's included amounts to the fixed assignee.

1. Portfolio results. Report every employee, including E012. AssignedCustomers
   counts all assigned profiles; PurchasingCustomers counts assigned customers
   with a Sale; RepeatCustomers counts those with at least two PurchaseInvoices
   in the analysis period. GrossPerAssignedCustomer uses AssignedCustomers as
   denominator; RepeatSharePct uses PurchasingCustomers. LargestCustomerSharePct
   is the largest assigned customer's GrossSales / portfolio GrossSales x 100.

   Output `outputs/task_4/employee_portfolios/`:
   `EmployeeID,EmployeeName,Team,Role,AssignedCustomers,PurchasingCustomers,RepeatCustomers,GrossSales,Cancellations,Adjustments,NetSales,GrossPerAssignedCustomer,RepeatSharePct,LargestCustomerSharePct`.

2. Team leaders. For each team, select all employees tied for the highest
   NetSales, including all ties when every member has zero NetSales.

   Output `outputs/task_4/team_leaders/`: `Team,EmployeeID,EmployeeName,NetSales`.

3. Attribution check. For each monetary metric, demonstrate that employee
   totals + unassigned identified-customer amounts + anonymous amounts equal
   the corresponding overall total.

   Output `outputs/task_4/sales_reconciliation/`, with one row for each of
   GrossSales, Cancellations, Adjustments and NetSales:
   `Metric,OverallTotal,AssignedTotal,UnassignedTotal,AnonymousTotal,Difference`.
   Difference is OverallTotal minus the sum of the three attributed totals
   and must equal zero. Include this table in the results report.

Use these results to identify leading portfolios and employees whose results
depend heavily on their single largest customer.

### Task 5 - Monthly bonus simulation

Situation: management proposes a monthly bonus based on net sales, with
a cancellation threshold and a maximum payout. Calculate the bonuses under
the following policy.

For every employee and each of the 23 analysis months, calculate G, C, A
and N from assigned customers. Apply these conditions in the following order:

| Condition | Status | Bonus |
| --- | --- | --- |
| G = 0 | NO_SALES | 0 |
| G > 0 and N < GBP 10,000 | TARGET_NOT_MET | 0 |
| G > 0, N >= GBP 10,000 and C/G > 0.10 | HIGH_CANCELLATIONS | 0 |
| Otherwise | ELIGIBLE | min(0.01 x N, GBP 500) |

The target is inclusive, cancellation incidence of exactly 10% is allowed,
and the cap is applied before rounding the final bonus to two decimals.
For zero-G months, CancellationPct remains empty even if cancellations exist.

Output `outputs/task_5/employee_monthly_bonus/`:
`EmployeeID,Month,GrossSales,Cancellations,Adjustments,NetSales,CancellationPct,BonusStatus,BonusAmount`.

Example calculations for one month:

| Employee | G | C | A | N | Status | Bonus (GBP) |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| E001 | 16,000 | 1,000 | 0 | 15,000 | ELIGIBLE | 150.00 |
| E002 | 15,000 | 2,000 | 0 | 13,000 | HIGH_CANCELLATIONS | 0.00 |
| E003 | 80,000 | 0 | 5,000 | 75,000 | ELIGIBLE | 500.00 |
| E004 | 5,000 | 0 | 0 | 5,000 | TARGET_NOT_MET | 0.00 |

Use these results to compare monthly bonus payouts and explain why employees
qualify or fail to qualify.

## Data source and attribution

Chen, D. (2012). Online Retail II. UCI Machine Learning Repository.
[Dataset and documentation](https://archive.ics.uci.edu/dataset/502/online+retail+ii).
DOI: [10.24432/C5CG6D](https://doi.org/10.24432/C5CG6D).
License: CC BY 4.0. Retain attribution and document conversion, normalisation
and any later changes to source records.
