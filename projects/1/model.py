#!/usr/bin/env python

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, GridSearchCV

#
# Dataset fields
#
numeric_features = ["if"+str(i) for i in range(1,14)]
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
#    ('scaler', StandardScaler())
])


categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

fields = ["id", "label"] + numeric_features + categorical_features

#
# Model pipeline
#

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ]
)

# Now we have a full prediction pipeline.
model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('linearregression', LinearRegression())
])
