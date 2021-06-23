dataset = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') 
    AS (region, country, state, city, month, day, year, avgtemperature);
citylatlong = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/citieslatlong.txt' USING PigStorage(',') 
    AS (lat, lon, city, country);
dataset_fix = FILTER dataset BY NOT (avgtemperature==-99 OR year<1995 OR month==0 OR year>2019);

data_lat = JOIN dataset_fix by (city, country), citylatlong by (city,country);
nh = FILTER data_lat BY (lat>0);
sh = FILTER data_lat BY (lat<0);

spring_nh = FILTER nh BY month>=3 and month<=5;
summer_nh = FILTER nh BY month>=6 and month<=8;
fall_nh = FILTER nh BY month>=9 and month<=11;
winter_nh = FILTER nh BY month>=12 and month<=2;

spring_sh = FILTER nh BY month>=9 and month<=11;
summer_sh = FILTER nh BY month>=12 and month<=2;
fall_sh = FILTER nh BY month>=3 and month<=5;
winter_sh = FILTER nh BY month>=6 and month<=8;

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

-- Que estacion es la que mas varia su temperatura promedio entre 1995 y 2019 (estacion_group_by_year)
-- En que hemisferio esta estacion fue en la que mas vario (estacion_sh o estacion_nh)
-- Cuales fueron las ciudades en que vario mas la temperatura para este hemisferio y estacion del año (estacion_sh o estacion_nh)


