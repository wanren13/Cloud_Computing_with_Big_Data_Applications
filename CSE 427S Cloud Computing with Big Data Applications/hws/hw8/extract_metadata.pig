-- Define an alias for the supplied Python script we 
-- use to parse the metadata from the MP3 files
DEFINE tagreader `readtags.py`;
-- DEFINE tagreader `readtags.py` SHIP('readtags.py');

-- load the list of MP3 files to analyze
calls = LOAD 'call_list.txt' AS (file:chararray);

-- Use the STREAM keyword to invoke our script, and parse
-- MP3 metadata, returning the fields shown here
metadata = STREAM calls THROUGH tagreader AS (path:chararray,
           category:chararray,
           agent_id:chararray,
           customer_id:chararray,
           timestamp:chararray);


-- TODO (A): Replace the hardcoded '2013-04' with
-- a parameter specified on the command line
by_month = FILTER metadata BY SUBSTRING(timestamp, 0, 7) == '$DATE';
-- DUMP '$DATE';

-- TODO (B): Count calls by category
category_group = GROUP by_month BY category;
category_count = FOREACH category_group GENERATE group AS category, COUNT(by_month) AS count;

-- TODO (C): Display the top three categories to the screen
top_category = ORDER category_count BY count DESC;
top_3_category = LIMIT top_category 3;
DUMP top_3_category;