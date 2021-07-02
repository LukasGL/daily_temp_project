

raw = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') AS (region, country, state, city, month:int, day:int, year:int, avgtemperature:float);

dataset_fix = FILTER raw BY (NOT avgtemperature==-99 OR year<1983);



cities= LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/citieslatlong.txt' USING PigStorage(',') AS (Latitud:float,Longitud:float,Ciudad,Pais);

data_lat = JOIN dataset_fix by (city, country), cities by (Ciudad,Pais);
norte = FILTER data_lat BY (Latitud > 0);
sur =  FILTER data_lat BY (Latitud < 0);



primavera_sur = FILTER sur BY (month == 10) OR (month == 11) OR (month == 12) ;
verano_sur = FILTER sur BY (month == 1) OR (month == 2) OR (month == 3) ;
otono_sur = FILTER sur BY (month == 4) OR (month == 5) OR (month == 6) ;
invierno_sur = FILTER sur BY (month == 7) OR (month == 8) OR (month == 9) ;

otono_norte = FILTER norte BY (month == 10) OR (month == 11) OR (month == 12) ;
invierno_norte = FILTER norte BY (month == 1) OR (month == 2) OR (month == 3) ;
primavera_norte = FILTER norte BY (month == 4) OR (month == 5) OR (month == 6) ;
verano_norte = FILTER norte BY (month == 7) OR (month == 8) OR (month == 9) ;


primavera = UNION primavera_norte, primavera_sur;
verano = UNION verano_norte, verano_sur;
invierno = UNION invierno_sur, invierno_norte;
otono = UNION otono_norte, otono_sur;

spring_group_by_year = GROUP primavera BY year;
spring_avg_by_year = FOREACH spring_group_by_year GENERATE group, AVG(primavera.avgtemperature);
summer_group_by_year = GROUP verano BY year;
summer_avg_by_year = FOREACH summer_group_by_year GENERATE group, AVG(verano.avgtemperature);
fall_group_by_year = GROUP otono BY year;
fall_avg_by_year = FOREACH fall_group_by_year GENERATE group, AVG(otono.avgtemperature);
winter_group_by_year = GROUP invierno BY year;

winter_avg_by_year = FOREACH winter_group_by_year GENERATE group, AVG(invierno.avgtemperature);




c95 = FILTER data_lat BY year == 1995;
c00 = FILTER data_lat BY year == 2000;
c_05 = FILTER data_lat BY year == 2005;
c_10 = FILTER data_lat BY year == 2010;
c_15 = FILTER data_lat BY year == 2015;
c_19 = FILTER data_lat BY year == 2019;


cy95 = GROUP c95 by city;
avg_95 = FOREACH cy95 GENERATE group, AVG(c95.avgtemperature) as temp;
o95 = ORDER avg_95 by temp;

c00 = FILTER data_lat BY year == 2000;
cy00 = GROUP c00 by city;
avg_00 = FOREACH cy00 GENERATE group, AVG(c00.avgtemperature) as temp;
o00 = ORDER avg_00 by temp;

c05 = FILTER data_lat BY year == 2005;
cy05 = GROUP c05 by city;
avg_05 = FOREACH cy05 GENERATE group, AVG(c05.avgtemperature) as temp;
o05 = ORDER avg_05 by temp;

c10 = FILTER data_lat BY year == 2010;
cy10 = GROUP c10 by city;
avg_10 = FOREACH cy10 GENERATE group, AVG(c10.avgtemperature) as temp;
o10 = ORDER avg_10 by temp;

c15 = FILTER data_lat BY year == 2015;
cy15 = GROUP c15 by city;
avg_15 = FOREACH cy15 GENERATE group, AVG(c15.avgtemperature) as temp;
o15 = ORDER avg_15 by temp;

c19 = FILTER data_lat BY year == 2019;
cy19 = GROUP c19 by city;
avg_19 = FOREACH cy19 GENERATE group, AVG(c19.avgtemperature) as temp;
o19 = ORDER avg_19 by temp;