

raw = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') AS (region, country, state, city, month, day, year, avgtemperature);

dataset_fix = FILTER raw BY (NOT avgtemperature==-99 OR year<1983);



cities= LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/citieslatlong.txt' USING PigStorage(',') AS (Latitud,Longitud,Ciudad,Pais);

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


primavera = UNION primavera_norte, primavera_sur
verano = UNION verano_norte, verano_sur
primavera = UNION primavera_sur, primavera_norte
otono = UNION otono_norte, otono_sur
