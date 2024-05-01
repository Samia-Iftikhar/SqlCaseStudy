
#A. Customer Nodes Exploration
#1. How many unique nodes are there on the Data Bank system?

select count(distinct node_id) as unique_nodes
from customer_nodes;

#2. What is the number of nodes per region?

select r.region_id,r.region_name, count(node_id) as nodesPerRegion
from regions r
join customer_nodes c on r.region_id=c.region_id
group by r.region_id,r.region_name
order by r.region_id;

#3. How many customers are allocated to each region?

select region_id, count(distinct customer_id) as no_of_customers
from customer_nodes
group by region_id;

#4. How many days on average are customers reallocated to a different node?

select avg(datediff(end_date, start_date)) as days_allocated
from customer_nodes
where end_date not like '9999-12-31';

#5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

with rows_ as (
select c.customer_id,
r.region_name, DATEDIFF(c.end_date, c.start_date) AS days_difference,
row_number() over (partition by r.region_name order by DATEDIFF(c.end_date, c.start_date)) AS rows_number,
COUNT(*) over (partition by r.region_name) as total_rows  
from
customer_nodes c JOIN regions r ON c.region_id = r.region_id
where c.end_date not like '%9999%'
)
Select region_name,
round(avg(Case when rows_number between (total_rows/2) and ((total_rows/2)+1) then days_difference end), 0) as Median,
max(case when rows_number = round((0.80 * total_rows),0) then days_difference end) as Percentile_80th,
max(case when rows_number = round((0.95 * total_rows),0) then days_difference end) as Percentile_95th
from rows_
group by region_name;

#B. Customer Transactions
#1. What is the unique count and total amount for each transaction type?

select  txn_type,count(*) as uniqueCount,
	sum(txn_amount) as totalAmount
from customer_transactions
group by txn_type;

#2. What is the average total historical deposit counts and amounts for all customers?

with deposit_summary as
(
select count(*) as totalCount, sum(txn_amount) as totalAmount
from customer_transactions
where txn_type like 'deposit'
group by customer_id
)
select avg(totalCount) as avgTotalCount, 
avg(totalAmount) as avgTotalAmount
from deposit_summary;

#3. For each month - how many Data Bank customers make more than 1
#deposit and either 1 purchase or 1 withdrawal in a single month?

with CTE as
(
select customer_id, monthname(txn_date) AS monthName,
count(Case When txn_type = 'deposit' Then 1 End) AS depositCount,
count(Case When txn_type = 'purchase' Then 1 End) AS purchaseCount,
count(Case When txn_type = 'withdrawal' Then 1 End) AS withdrawalCount
from customer_transactions
group by customer_id, monthname(txn_date)
)
select monthName, count(distinct customer_id) AS customerCount
from CTE
where depositCount > 1
      and (purchaseCount > 0 OR withdrawalCount > 0)
group by monthName;

#4. What is the closing balance for each customer at the end of the month?

With cte as
(
select customer_id, month(txn_date) as month1,monthname(txn_date) as monthN,
sum(case 
when txn_type = 'deposit' then txn_amount
else -txn_amount 
end) as totalAmount
from customer_transactions
group by customer_id, month(txn_date),monthname(txn_date)
)
Select cte.customer_id, cte.month1 as month, cte.monthN as monthName,
Sum(cte.totalAmount) over (partition by cte.customer_id order by cte.month1 asc) as closingBalance
from cte;

#5. What is the percentage of customers who increase their closing balance
#by more than 5%?

With cte as
(
select customer_id, last_day(txn_date) as endDate,
sum(case 
when txn_type = 'deposit' then txn_amount
else -txn_amount 
end) as totalAmount
from customer_transactions
group by customer_id, last_day(txn_date)
),

cte2 as
(
Select cte.customer_id, cte.endDate,
Sum(cte.totalAmount) over (partition by cte.customer_id order by cte.endDate) as closingBalance
from cte
),

cte3 as
(
select customer_id, endDate, closingBalance,
lag(closingBalance) over (partition by customer_id order by endDate) AS prev_closingBalance,
100 * (closingBalance - lag(closingBalance) over (partition by customer_id order by endDate)) / NULLIF(lag(closingBalance) over (partition by customer_id order by endDate), 0) AS pct_increase
from cte2
)

select 100 * count(distinct customer_id) / (select count(distinct customer_id) from customer_transactions) AS pOfCustomers
from cte3
where pct_increase > 5;

#C. Data Allocation Challenge
# running customer balance column that includes the impact each transaction

Select customer_id, txn_date, txn_type, txn_amount,
Sum(Case
 When txn_type = 'deposit' Then txn_amount
 When txn_type = 'withdrawal' Then -txn_amount
 When txn_type = 'purchase' Then -txn_amount
 Else 0
 End) 
Over (partition by customer_id order by txn_date) as runningBalance
from customer_transactions;

# customer balance at the end of each month

Select customer_id, monthname(txn_date) AS monthName,
Sum(Case
 When txn_type = 'deposit' Then txn_amount
 when txn_type = 'withdrawal' Then -txn_amount
 when txn_type = 'purchase' Then -txn_amount
 else 0
 end) as closingBalance
from customer_transactions
group by customer_id, monthname(txn_date)
order by customer_id, monthname(txn_date);

# minimum, average and maximum values of the running balance for each customer

With RBalance as
(
Select customer_id, txn_date, txn_type, txn_amount,
Sum(Case 
when txn_type = 'deposit' then txn_amount
when txn_type = 'withdrawal' then -txn_amount
when txn_type = 'purchase' then -txn_amount
else 0
end) 
over (partition by customer_id order by txn_date) AS runningBalance
from customer_transactions
)
Select customer_id, avg(runningBalance) AS avgRunningBalance,
	min(runningBalance) AS minRunningBalance,
	max(runningBalance) AS maxRunningBalance
from RBalance
group by customer_id;
