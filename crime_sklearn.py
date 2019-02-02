#!/usr/bin/env python2
# -*- coding: utf-8 -*-


import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas import DatetimeIndex
from sklearn.preprocessing import Imputer

dataset = pd.read_csv('samplecrime.csv')
dataset['Day'] = DatetimeIndex(dataset['Date']).day
dataset['Month']=DatetimeIndex(dataset['Date']).month
dataset['Hour']=DatetimeIndex(dataset['Date']).hour
X = dataset.loc[:,['Day','Month','Hour','Primary Type','Location Description','Domestic','Year']].values
y = dataset.loc[:,['Arrest']].values



from sklearn.preprocessing import LabelEncoder, OneHotEncoder

labelencoder = LabelEncoder()
X[:,5] = labelencoder.fit_transform(X[:,5])


onehotencoder = OneHotEncoder(categorical_features = [3])
X = onehotencoder.fit_transform(X).toarray()

onehotencoder = OneHotEncoder(categorical_features = [4])
X = onehotencoder.fit_transform(X).toarray()

labelencoder = LabelEncoder()
y = labelencoder.fit_transform(y)