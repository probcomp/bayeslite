# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2017, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import pandas as pd

from sklearn.linear_model import LinearRegression

def regress_ols(target_values, given_values, given_variables, stattypes):
    X = pd.DataFrame(given_values, columns=given_variables)
    # Detect the nominal variables.
    nominal_variables = [
        variable for variable, stattype in zip(given_variables, stattypes)
        if stattype == 'nominal'
    ]
    # Dummy code the nominal variables.
    prefix = {var: '%s_dum' % (var,) for var in nominal_variables}
    X_coded = pd.get_dummies(X, columns=nominal_variables, prefix=prefix)
    # Find nominal columns to drop, and drop them (for correct dummy coding, K
    # categories are encoded using K-1 vector).
    drop = [
        filter(lambda c: c.startswith('%s_dum' % (var,)), X_coded.columns)[0]
        for var in nominal_variables
    ]
    X_coded.drop(drop, inplace=True, axis=1)
    # Check if only 1 column with 1 unique values.
    if len(X_coded.columns) == 0 or len(X_coded) == 0:
        raise ValueError('Not enough data for regression')
    # Fit the regression.
    linreg = LinearRegression()
    linreg.fit(X_coded, target_values)
    # Build and return variables and their coefficients.
    intercept = [('intercept', linreg.intercept_)]
    variables_regressed = zip(X_coded.columns, linreg.coef_)
    variables_dropped = zip(drop, [0]*len(drop))
    return intercept + variables_regressed + variables_dropped
