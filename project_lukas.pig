-- This script finds the actors/actresses with the highest number of good movies

dataset = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') AS (region, country, state, city, month, day, year, avgtemperature);
citylatlong = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/citieslatlong.txt' USING PigStorage(',') AS (lat, lon, city, country);
dataset_fix = FILTER dataset BY NOT (avgtemperature==-99 OR year<1995 OR month==0);

data_lat = JOIN dataset_fix by (city, country), citylatlong by (city,country);
nh = FILTER data_lat BY (lat>0);
sh = FILTER data_lat BY (lat<0);

spring_nh = FILTER nh BY month>=3 and month<=5;
summer_nh = FILTER nh BY month>=6 and month<=8;
fall_nh = FILTER nh BY month>=9 and month<=11;
winter_nh = FILTER nh BY month>=12 and month<=2;

spring_sh = FILTER sh BY month>=9 and month<=11;
summer_sh = FILTER sh BY month>=12 and month<=2;
fall_sh = FILTER sh BY month>=3 and month<=5;
winter_sh = FILTER sh BY month>=6 and month<=8;

spring_all = UNION spring_nh, spring_sh;
summer_all = UNION summer_nh, summer_sh;
fall_all = UNION fall_nh, fall_sh;
winter_all = UNION winter_nh, winter_sh;

-- AVG BY YEAR BY SEASON
spring_group_by_year = GROUP spring_all BY year;
spring_avg_by_year = FOREACH spring_group_by_year GENERATE group, AVG(spring_all.avgtemperature);
summer_group_by_year = GROUP summer_all BY year;
summer_avg_by_year = FOREACH summer_group_by_year GENERATE group, AVG(summer_all.avgtemperature);
fall_group_by_year = GROUP fall_all BY year;
fall_avg_by_year = FOREACH fall_group_by_year GENERATE group, AVG(fall_all.avgtemperature);
winter_group_by_year = GROUP winter_all BY year;
winter_avg_by_year = FOREACH winter_group_by_year GENERATE group, AVG(winter_all.avgtemperature);


-- TOP 20 cities with max in summer
summer_city_and_temp = FOREACH summer_all GENERATE dataset_fix:city as city, dataset_fix:avgtemperature as avgtemperature;
summer_sort_by_city = ORDER summer_city_and_temp BY avgtemperature DESC;
summer_limit = LIMIT summer_sort_by_city 20;

-- TOP 20 cities with min in winter
winter_city_and_temp = FOREACH winter_all GENERATE city, avgtemperature;
winter_sort_by_city = ORDER winter_city_and_temp ASC;
winter_limit = LIMIT winter_sort_by_city 20;

DUMP summer_limit;

group_year_region = GROUP dataset_fix BY (year, region);
average_temp_by_year_region = FOREACH group_year_region {
    unique_cities = DISTINCT dataset_fix.city;
    GENERATE group.$0 as year, group.$1 as region, AVG(dataset_fix.avgtemperature) as AvgTemperatureByYear, COUNT(unique_cities) as city_count;
};

group_region = GROUP average_temp_by_year_region BY region;
-- FOREACH group_region GENERATE group, MAX(average_temp_by_year_region) as max_temp, MIN(average_temp_by_year_region);
avg_temp_diff_by_region = FOREACH group_region {
    GENERATE group, MAX(average_temp_by_year_region.AvgTemperatureByYear) - MIN(average_temp_by_year_region.AvgTemperatureByYear) AS temp_diff;
};
ordered_region_diff = ORDER avg_temp_diff_by_region by temp_diff;

-- Hemisferio norte
-- Primavera mes 3 a 5
-- Verano mes 6 a 8
-- Otoño mes 9 a 11
-- Invierno 12 a 2

-- Hemisferio sur
-- Primavera mes 9 a 11
-- Verano mes 12 a 2
-- Otoño mes 3 a 5
-- Invierno 6 a 8