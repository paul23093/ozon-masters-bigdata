CREATE EXTERNAL TABLE hw2_test (
id int,
if1 int,
if2 int,
if3 int,
if4 int,
if5 int,
if6 int,
if7 int,
if8 int,
if9 int,
if10 int,
if11 int,
if12 int,
if13 int,
cf1 string,
cf2 string,
cf3 string,
cf4 string,
cf5 string,
cf6 string,
cf7 string,
cf8 string,
cf9 string,
cf10 string,
cf11 string,
cf12 string,
cf13 string,
cf14 string,
cf15 string,
cf16 string,
cf17 string,
cf18 string,
cf19 string,
cf20 string,
cf21 string,
cf22 string,
cf23 string,
cf24 string,
cf25 string,
cf26 string,
day_number string
)
row format delimited
fields terminated by '\t'
stored as textfile
LOCATION '/datasets/criteo_test_large_features';

select *
from hw2_test
limit 10;