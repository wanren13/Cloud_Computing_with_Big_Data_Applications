-- data = LOAD 'test_ad_data.txt'
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

data = FILTER data BY was_clicked IS NOT NULL;

grouped = GROUP data ALL;

cost = FOREACH grouped GENERATE MAX(data.cpc) * 50000 AS max_cost;

DUMP cost;