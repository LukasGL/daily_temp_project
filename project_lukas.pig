-- This script finds the actors/actresses with the highest number of good movies

dataset = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') AS (region, country, state, city, month:int, day:int, year:int, avgtemperature:float);
citylatlong = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/citieslatlong.txt' USING PigStorage(',') AS (lat:float, lon:float, city, country);
dataset_fix = FILTER dataset BY NOT (avgtemperature==-99 OR year<1995 OR month==0 OR year==2020);

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

-- TOP 10 ciudades con mayor variacion de temperaturas
summer_var_by_city = GROUP summer_all BY (dataset_fix::city, dataset_fix::country);
summer_var_value = FOREACH summer_var_by_city GENERATE group.$0, group.$1, (MAX(summer_all.avgtemperature)-MIN(summer_all.avgtemperature)) as vartemperature;
summer_var_sort = ORDER summer_var_value BY vartemperature DESC;
summer_var_limit = LIMIT summer_var_sort 5;

(Great Falls,US,63.5)
(Rabat,Morocco,63.2)
(Rapid City,US,53.100002)
(Pocatello,US,51.5)
(Ulan-bator,Mongolia,50.8)


-- Plantilla temperaturas de ciudad verano
city_summer_temp = FILTER summer_all BY dataset_fix::city=='Ulan-bator' AND dataset_fix::country=='Mongolia';
city_group_by_year = GROUP city_summer_temp BY year;
city_avg_by_year = FOREACH city_group_by_year GENERATE group, AVG(city_summer_temp.avgtemperature);
(1995,61.410869805709176)
(1996,67.10123453022521)
(1997,63.776086724322774)
(1998,64.85434818267822)
(1999,63.81739106385604)
(2000,66.3673911716627)
(2001,67.256521805473)
(2002,64.35287335275234)
(2003,68.85760862930961)
(2004,63.51086956521739)
(2005,64.71956518422003)
(2006,68.49130443904711)
(2007,69.8560439308921)
(2008,65.39011009970864)
(2009,63.73478267503821)
(2010,63.09347853453263)
(2011,65.61304361923881)
(2012,68.77717353986657)
(2013,67.0217398353245)
(2014,65.28152200450067)
(2015,68.26956500177798)
(2016,66.0021740871927)
(2017,68.87065211586331)
(2018,67.15760873711628)
(2019,64.84021738301153)

-- Plantilla var by region in summer
summer_by_region = GROUP summer_all BY (region, year);
region_avg_by_year = FOREACH summer_by_region GENERATE group.$0, group.$1, AVG(summer_all.avgtemperature);



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