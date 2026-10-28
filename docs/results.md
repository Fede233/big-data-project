# Results

## Overview

The analysis covers January 2010-November 2011. Transactions come from UCI
Online Retail II; customer profile attributes, employees and assignments are
simulated. All amounts are in GBP.

## Quality

The provided quality checks give these counts for the full input:

| Class | Rows |
| --- | ---: |
| Sale | 1,019,654 |
| Cancellation | 19,164 |
| Other negative adjustment | 0 |
| Excluded | 6,030 |
| Invalid | 0 |

Of the financially included rows, 968,374 fall inside the analysis period.

There are 229,327 included rows without a customer ID across the full input.
They count towards overall sales but cannot be assigned to a customer or
employee. The 11,812 duplicate-row excess is retained as requested.
There are 297 unassigned customers; E012 has no clients.

## Task 1 — Sales trends

Sales and cancellations were grouped by country and month. The outputs contain
549 country/month rows and 43 country growth rows.

Total gross sales are 19,069,245.46. After 1,234,713.91 in cancellations and no
other adjustments, net sales are 17,834,531.55.

Comparing January-November in both years:

- Australia: net sales rise from 29,384.40 to 136,072.17 (+363.08%).
- France: from 118,951.59 to 180,779.71 (+51.98%).
- United Kingdom: from 7,259,317.77 to 7,144,452.43 (-1.58%).

Australia and France have the largest increases in absolute amounts. Over the
full 23 months, UK cancellations reduce gross sales by 1,042,444.19.

## Task 2 — Products

The outputs contain 4,792 products, 231 watchlist entries and 46 leading
country/product pairs. There are more leaders than the 43 countries because
all tied products are included.

Product 22423 has gross sales of 325,077.40 across 3,859 purchase invoices,
with cancellations of 16,466.85 (5.07%). Codes M and 23166 are on the watchlist:
their cancellation percentages are 123.89% and 95.13%.

These codes deserve a closer look. A value above 100% is possible because
cancellations in the period can refer to older sales; item codes can also
represent administrative entries.

## Task 3 — Customers

All 5,942 customer profiles appear in the output, together with four service
profile groups. There are 5,761 purchasing customers and 4,126 repeat customers,
counting distinct invoices rather than invoice lines.

Retail/Standard is the largest group: 3,122 customers and net sales of
7,626,328.95. Repeat shares among purchasing customers are:

- Retail/Priority: 72.79%.
- Retail/Standard: 71.51%.
- Wholesale/Priority: 68.89%.
- Wholesale/Standard: 71.78%.

The shares are fairly close. Since service profiles are simulated, these
numbers do not show that one service plan causes more repeat purchases.

## Task 4 — Employee portfolios

The outputs contain 12 employee portfolios, three team leaders and four
reconciliation rows. The highest net sales in each team are:

- TeamA: E003, 1,992,790.42.
- TeamB: E008, 1,988,541.87.
- TeamC: E009, 1,200,430.18.

E003 and E008 depend on their largest customer for 24.35% and 24.15% of gross
sales. This is worth considering alongside their totals. E012 is included
with zero counts and amounts; ratios with a zero denominator are empty.

The attribution check gives the following results:

| Metric | OverallTotal | AssignedTotal | UnassignedTotal | AnonymousTotal | Difference |
| --- | ---: | ---: | ---: | ---: | ---: |
| GrossSales | 19,069,245.46 | 15,440,009.55 | 786,676.66 | 2,842,559.25 | 0.00 |
| Cancellations | 1,234,713.91 | 869,712.64 | 19,100.98 | 345,900.29 | 0.00 |
| Adjustments | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| NetSales | 17,834,531.55 | 14,570,296.91 | 767,575.68 | 2,496,658.96 | 0.00 |

All four differences are zero: assigned, unassigned and anonymous amounts add
up to the overall totals.

## Task 5 — Monthly bonuses

The output has 276 rows: one for each of the 12 employees in each of the
23 months. There are 225 eligible employee/months, 28 rejected for high
cancellations and 23 without sales, all belonging to E012. No active
employee/month falls below the net-sales target.

The total simulated bonus is 99,114.83, and 109 rows reach the 500 monthly cap.
Reaching the sales target alone is therefore not enough to receive a bonus:
the cancellation threshold also matters.
