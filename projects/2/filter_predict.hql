ADD FILE projects/2/predict.py;
ADD FILE projects/2/model.py;
ADD FILE 2.joblib;
INSERT INTO hw2_pred
SELECT TRANSFORM(*) USING 'predict.py' AS (id, pred)
FROM hw2_test
WHERE if1 != '\N'
AND if1 between 20 and 40;