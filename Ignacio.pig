raw_temperatures = LOAD 'hdfs://cm:9000/uhadoop2021/group_46/project/city_temperature.csv' USING PigStorage(',') AS (Region, Country, State, City, Month, Day, Year, AvgTemperature);
cleaned_data = FILTER raw_temperatures BY NOT (AvgTemperature==-99 OR Year<1995 OR Year>2019);

temp_group = GROUP raw_temperatures ALL;

temp_count = FOREACH temp_group GENERATE COUNT(raw_temperatures);  
-- 2.906.328

unique_years = distinct(FOREACH dataset GENERATE $6); 
-- (1995)(1996)(1997)(1998)(1999)(2000)(2001)(2002)(2003)(2004)(2005)(2006)(2007)(2008)(2009)(2010)(2011)(2012)
-- (2013)(2014)(2015)(2016)(2017)(2018)(2019)

unique_months = distinct(FOREACH cleaned_data GENERATE $4);
-- (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11)(12)

unique_days = distinct(FOREACH cleaned_data GENERATE $5); 
-- (1)(2)(3)(4)(5)(6)(7)(8)(9)(10)(11)(12)(13)(14)(15)(16)(17)(18)(19)(20)(21)(22)(23)(24)(25)(26)(27)(28)(29)(30)(31)

unique_regions = distinct(FOREACH cleaned_data GENERATE $0);
-- (Asia)(Africa)(Europe)(Region)(Middle East)(North America)(Australia/South Pacific)(South/Central America & Carribean)

unique_countries = distinct(FOREACH cleaned_data GENERATE Country);
group_unique_countries = GROUP unique_countries ALL;
count_countries = FOREACH group_unique_countries GENERATE COUNT(unique_countries);
-- (125)

group_regions = GROUP cleaned_data BY Region;
count_data_by_region = FOREACH group_regions GENERATE FLATTEN(group) AS (Region), COUNT($1);
-- (Asia,301754)(Africa,217714)(Europe,363761)(Middle East,119210)(North America,1527006)
-- (Australia/South Pacific,54405)(South/Central America & Carribean,204063)

group_regions = GROUP cleaned_data BY Region;
count_countries_by_region = FOREACH  group_regions {
    unique_countries_2 = DISTINCT cleaned_data.Country;
    GENERATE group, COUNT(unique_countries_2) AS country_count;
};
-- (Asia,25)(Africa,28)(Europe,34)(Middle East,11)(North America,3)
-- (Australia/South Pacific,2)(South/Central America & Carribean,22)

group_regions = GROUP cleaned_data BY Region;
count_cities_by_region = FOREACH  group_regions {
    unique_cities = DISTINCT cleaned_data.City;
    GENERATE group, COUNT(unique_cities) AS city_count;
};
-- (Asia,35)(Africa,29)(Europe,45)(Middle East,14)(North America,167)
-- (Australia/South Pacific,6)(South/Central America & Carribean,25)

group_year = GROUP cleaned_data BY Year;
average_temp_by_year = FOREACH group_year {
    unique_cities = DISTINCT cleaned_data.City;
    GENERATE group, AVG(cleaned_data.AvgTemperature) as AvgTemperatureByYear, COUNT(unique_cities) as city_count;
};
-- (1995,59.42165899850195,314)(1996,58.67483013110545,316)(1997,59.133616934627156,319)(1998,60.870439776061566,318)
-- (1999,60.32768724758826,318)(2000,59.6990260053082,317)(2001,60.32628017731203,318)(2002,60.256345937192386,319)
-- (2003,59.99836021105516,320)(2004,60.09293437001006,317)(2005,60.52374036157704,315)(2006,61.02458822602279,317)
-- (2007,60.75195264180986,311)(2008,59.873358823320906,307)(2009,60.102735955106525,307)(2010,60.642856514316954,312)
-- (2011,60.72023450496837,304)(2012,61.40452930328811,300)(2013,60.07836302935792,299)(2014,60.178842017429865,297)
-- (2015,61.66943796871073,289)(2016,62.03345770326009,287)(2017,61.74111030717623,287)(2018,61.23136513220134,287)
-- (2019,61.096138364181336,285)

group_year_region = GROUP cleaned_data BY (Year, Region);
average_temp_by_year_region = FOREACH group_year_region {
    unique_cities = DISTINCT cleaned_data.City;
    GENERATE group.$0 as Year, group.$1 as Region,  AVG(cleaned_data.AvgTemperature) as AvgTemperatureByYear, COUNT(unique_cities) as city_count;
};

group_region = GROUP average_temp_by_year_region BY Region;
avg_temp_diff_by_region = FOREACH group_region {
    GENERATE group, MAX(average_temp_by_year_region.AvgTemperatureByYear) - MIN(average_temp_by_year_region.AvgTemperatureByYear) AS temp_diff;
};
-- (Asia,1.8242409641028843)(Africa,1.9128405812278544)(Europe,2.8828214151892766)(Middle East,5.14946353750959)(North America,4.1679782360126865)
-- (Australia/South Pacific,2.366877943854412)(South/Central America & Carribean,3.2510453075788632)

-- 0-1 Celsius = 32-33.8 => 1.8 diff

-- Northern Hemisphere
-- spring runs from March 1 to May 31;
-- summer runs from June 1 to August 31;
-- fall (autumn) runs from September 1 to November 30; and
-- winter runs from December 1 to February 28 (February 29 in a leap year).

-- Southern Hemisphere
-- spring starts September 1 and ends November 30;
-- summer starts December 1 and ends February 28 (February 29 in a Leap Year);
-- fall (autumn) starts March 1 and ends May 31; and
-- winter starts June 1 and ends August 31;


