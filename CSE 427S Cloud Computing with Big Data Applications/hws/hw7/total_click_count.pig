-- Load only the ad_data1 and ad_data2 directories
-- data = LOAD '../test_ad_data.txt' 
-- data = LOAD '/dualcore/ad_data[12]' -- use this one for root directory 
data = LOAD 'dualcore/ad_data[12]' 
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

-- Include only records where the ad was clicked
clicked = FILTER data BY was_clicked == 1;

-- A: Group everything so we can call the aggregate function
grouped = GROUP clicked ALL;

-- B: Count the records 
counted = FOREACH grouped GENERATE COUNT(clicked.campaign_id) AS clicks;

-- C: Display the result to the screen

DUMP counted;
