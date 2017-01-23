data = LOAD 
        -- 'test_orders.txt'
        '/dualcore/orders' 
        AS (
            order_id:int,
            cust_id:int,
            order_dtm:chararray
        );
-- DUMP data;
/*
 * Include only records where the 'order_dtm' field matches
 * the regular expression pattern:
 *
 *   ^       = beginning of string
 *   2013    = literal value '2013'
 *   0[2345] = 0 followed by 2, 3, 4, or 5
 *   -       = a literal character '-'
 *   \\d{2}  = exactly two digits
 *   \\s     = a single whitespace character
 *   .*      = any number of any characters
 *   $       = end of string
 *
 * If you are not familiar with regular expressions and would
 * like to know more about them, see the Regular Expression 
 * Reference at the end of the Exercise Manual.
 */
recent = FILTER data by order_dtm matches '^2013-0[2345]-\\d{2}\\s.*$';
-- DUMP recent;

-- TODO (A): Create a new relation with just the order's year and month

year_month = FOREACH recent GENERATE SUBSTRING(order_dtm, 0, 4) as year, SUBSTRING(order_dtm, 5, 7) as month;
-- year_month = LIMIT year_month 10;
-- DUMP year_month;
-- DESCRIBE  year_month;


-- TODO (B): Count the number of orders in each month
groupedbymonth = GROUP year_month BY month;
counted = FOREACH groupedbymonth GENERATE group AS month, COUNT(year_month) as count;

-- TODO (C): Display the count by month to the screen.
DESCRIBE counted;
DUMP counted;


