USE paul23093;
ADD FILE projects/2/predict.py;
ADD FILE projects/2/model.py;
ADD FILE 2.joblib;
SELECT TRANSFORM(*) USING 'python3 predict.py' AS (id, pred)
FROM hw2_test
WHERE if1 != '\N'
AND if1 between 20 and 40;