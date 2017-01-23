-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.

-- data = LOAD 'test_ad_data.txt' 
-- data = LOAD '/dualcore/ad_data[12]/part*' -- use this one for root directory
data = LOAD 'dualcore/ad_data[12]/part*'
        AS (
             campaign_id:chararray,
             date:chararray, 
             time:chararray,
             keyword:chararray, 
             display_site:chararray, 
             placement:chararray, 
             was_clicked:int, 
             cpc:int
           );

-- TODO (B): Include only records where was_clicked has a value of 1
fltdata = FILTER data BY was_clicked == 1;

-- TODO (C): Group the data by the appropriate field
grpdata = GROUP fltdata BY keyword;

/* TODO (D): Create a new relation which includes only the 
 *           display site and the total cost of all clicks 
 *           on that site
 */
sumdata = FOREACH grpdata GENERATE group AS keyword, SUM(fltdata.cpc) AS cost;

-- TODO (E): Sort that new relation by cost (ascending)
orddata = ORDER sumdata BY cost DESC;

-- TODO (F): Display just the first four records to the screen
limitdata = LIMIT orddata 3;
DUMP limitdata;