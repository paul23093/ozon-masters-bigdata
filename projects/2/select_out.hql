CREATE EXTERNAL TABLE paul23093_hiveout(
id int,
pred float
)
STORED TEXTFILE
LOCATION 'paul23093_hiveout';

INSERT OVERWRITE TABLE paul23093_hiveout
SELECT *
FROM hw2_pred;