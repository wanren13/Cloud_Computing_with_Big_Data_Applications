DEFINE ROW_COUNT(R) RETURNS rows {
    rgroup = GROUP $R ALL;
    $rows = FOREACH rgroup GENERATE COUNT ($R);
}

-- sh rm -rf platinum;
-- sh rm -rf gold;
-- sh rm -rf silver;

fs -rm -r -f /dualcore/platinum;
fs -rm -r -f /dualcore/gold;
fs -rm -r -f /dualcore/silver;

-- load the data sets
-- orders = LOAD 'orders'
orders = LOAD '/dualcore/orders' 
        AS (
            order_id:int,
            cust_id:int,
            order_dtm:chararray
        );

-- details = LOAD 'order_details'
details = LOAD '/dualcore/order_details' 
        AS (
            order_id:int,
            prod_id:int
        );

-- products = LOAD 'products' 
products = LOAD '/dualcore/products' 
        AS (
            prod_id:int,
            brand:chararray,
            name:chararray,
            price:int,
            cost:int,
            shipping_wt:int
        );


/*
 * TODO: Find all customers who had at least five orders
 *       during 2012. For each such customer, calculate 
 *       the total price of all products he or she ordered
 *       during that year. Split those customers into three
 *       new groups based on the total amount:
 *
 *        platinum: At least $10,000
 *        gold:     At least $5,000 but less than $10,000
 *        silver:   At least $2,500 but less than $5,000
 *
 *       Write the customer ID and total price into a new
 *       directory in HDFS. After you run the script, use
 *       'hadoop fs -getmerge' to create a local text file
 *       containing the data for each of these three groups.
 *       Finally, use the 'head' or 'tail' commands to check 
 *       a few records from your results, and then use the  
 *       'wc -l' command to count the number of customers in 
 *       each group.
 */

-- Find orders in 2012
orders_2012 = FILTER orders BY order_dtm MATCHES '^2012.*';

-- row = ROW_COUNT(orders_2012);
-- DUMP row;

-- Shrink products
products = FOREACH products GENERATE prod_id, price;

-- Find customer groups by custom ID
customers = GROUP orders_2012 BY cust_id;

-- row = ROW_COUNT(customers);
-- DUMP row;

-- Count number of orders for each customer
customer_order_count = FOREACH customers GENERATE 
                        group AS cust_id, 
                        COUNT(orders_2012) AS order_cnt;

-- row = ROW_COUNT(customer_order_count);
-- DUMP row;

-- Customers made at least 5 purchases
qualified_customers = FILTER customer_order_count BY order_cnt >= 5;

-- row = ROW_COUNT(qualified_customers);
-- DUMP row;

valuable_customers = JOIN qualified_customers BY cust_id, orders_2012 BY cust_id;
valuable_customers = JOIN valuable_customers BY orders_2012::order_id, details BY order_id;
valuable_customers = JOIN valuable_customers BY details::prod_id, products BY prod_id;
-- DESCRIBE valuable_customers;

-- Shrink valuable_customers 
valuable_customers = FOREACH valuable_customers GENERATE 
                     valuable_customers::valuable_customers::qualified_customers::cust_id AS cust_id, 
                     products::price AS price;
-- DESCRIBE valuable_customers;

valuable_customers_group = GROUP valuable_customers BY cust_id;

customers_purchase = FOREACH valuable_customers_group GENERATE 
                     group, SUM(valuable_customers.price) as purchase;

-- customers_purchase = LIMIT customers_purchase 10;
-- DESCRIBE customers_purchase;
-- DUMP customers_purchase;

SPLIT customers_purchase INTO 
platinum IF purchase >= 1000000,
gold IF purchase >= 500000 AND purchase < 1000000,
silver IF purchase >= 250000 AND purchase < 500000;


-- STORE platinum INTO 'platinum';
-- STORE gold INTO 'gold';
-- STORE silver INTO 'silver';

-- /*
STORE platinum INTO '/dualcore/platinum';
STORE gold INTO '/dualcore/gold';
STORE silver INTO '/dualcore/silver';

fs -getmerge /dualcore/platinum platinum.txt
fs -getmerge /dualcore/gold gold.txt
fs -getmerge /dualcore/silver silver.txt
sh wc -l platinum.txt gold.txt silver.txt
-- */