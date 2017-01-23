orders = LOAD '/dualcore/orders' 
        AS (
            order_id:int,
            cust_id:int,
            order_dtm:chararray
        );

details = LOAD '/dualcore/order_details' 
        AS (
            order_id:int,
            prod_id:int
        );

-- include only the months we want to analyze
recent = FILTER orders BY order_dtm matches '^2013-0[2345]-.*$';  -- 363610

-- include only the product we're advertising
tablets = FILTER details BY prod_id == 1274348;  -- 119801

-- recent = GROUP recent ALL;
-- tablets = GROUP tablets ALL;
-- count_recent = FOREACH recent GENERATE COUNT(recent);
-- count_tablets = FOREACH tablets GENERATE COUNT(tablets);
-- DUMP count_recent;
-- DUMP count_tablets;

-- TODO (A): Join the two relations on the order_id field
joined = JOIN tablets BY order_id, recent BY order_id;

-- TODO (B): Create a new relation containing just the month
joined_month = FOREACH joined GENERATE SUBSTRING(order_dtm, 5, 7) as mon;

-- TODO (C): Group by month and then count the records in each group
grouped = GROUP joined_month BY mon;

-- TODO (D): Display the results to the screen
counted = FOREACH grouped GENERATE group as month, COUNT(joined_month) as count;
DESCRIBE  counted;
DUMP counted;