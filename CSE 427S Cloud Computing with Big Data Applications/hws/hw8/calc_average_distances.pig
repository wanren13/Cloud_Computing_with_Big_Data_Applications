-- Register DataFu and define an alias for the function
REGISTER '/usr/lib/pig/datafu-*.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;


cust_locations = LOAD '/dualcore/distribution/cust_locations/'
                   AS (zip:chararray,
                       lat:double,
                       lon:double);

warehouses = LOAD '/dualcore/distribution/warehouses.tsv'
                   AS (zip:chararray,
                       lat:double,
                       lon:double);
             


-- Create a record for every combination of customer and
-- proposed distribution center location.
combinations = CROSS warehouses, cust_locations;

-- Calculate the distance from the customer to the warehouse
warehouse_dists = FOREACH combinations GENERATE 
                        warehouses::zip AS zip, 
                        DIST(warehouses::lat, warehouses::lon, cust_locations::lat, cust_locations::lon) AS dist;

-- Calculate the average distance for all customers to each warehouse
warehouse_group = GROUP warehouse_dists BY zip;
warehouse_avg_dists = FOREACH warehouse_group GENERATE 
                        group AS zip,
                        AVG(warehouse_dists.dist) AS avg_dist;

-- Display the result to the screen
lowest = ORDER warehouse_avg_dists BY avg_dist ASC;
DESCRIBE lowest;
DUMP lowest;