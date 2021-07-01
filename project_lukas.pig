-- This script finds the actors/actresses with the highest number of good movies

dataset = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') AS (region, country, state, city, month:int, day:int, year:int, avgtemperature:float);
citylatlong = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/citieslatlong.txt' USING PigStorage(',') AS (lat:float, lon:float, city, country);
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

-- AVG BY YEAR BY SEASON -- FUNCIONA
spring_group_by_year = GROUP spring_all BY year;
spring_avg_by_year = FOREACH spring_group_by_year GENERATE group, AVG(spring_all.avgtemperature);
summer_group_by_year = GROUP summer_all BY year;
summer_avg_by_year = FOREACH summer_group_by_year GENERATE group, AVG(summer_all.avgtemperature);
fall_group_by_year = GROUP fall_all BY year;
fall_avg_by_year = FOREACH fall_group_by_year GENERATE group, AVG(fall_all.avgtemperature);
winter_group_by_year = GROUP winter_all BY year;
winter_avg_by_year = FOREACH winter_group_by_year GENERATE group, AVG(winter_all.avgtemperature);

-- TOP 20 distinct cities with max in summer -- FUNCIONA
summer_group_by_city = GROUP summer_all BY (dataset_fix::city, dataset_fix::country);
summer_max_by_city = FOREACH summer_group_by_city GENERATE group, MAX(summer_all.avgtemperature) as maxtemperature;
summer_all_sort = ORDER summer_max_by_city BY maxtemperature DESC;
summer_all_limit = LIMIT summer_all_sort 20;

-- TOP 20 distinct cities with min in winter -- FUNCIONA
winter_group_by_city = GROUP winter_all BY (dataset_fix::city, dataset_fix::country);
winter_max_by_city = FOREACH winter_group_by_city GENERATE group, MIN(winter_all.avgtemperature) as mintemperature;
winter_all_sort = ORDER winter_max_by_city BY mintemperature ASC;
winter_all_limit = LIMIT winter_all_sort 20;
DUMP winter_all_limit;


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