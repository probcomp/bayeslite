# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
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

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import Imputer

class SatRandomForest(object):
    """
    A "foreign predictor" trained on the satellites.csv dataset. The
    SatRandomForest is trained to predict
        `Type_of_Orbit`
    conditioned on
        `Perigee_km, `Apogee_km`, `Eccentricity`, `Period_minutes`,
        `Launch_Mass_kg`, `Power_watts`, `Anticipated_Lifetime`.

    Example
    -------
    >> sat_df = pd.read_csv('/your/path/to/bdbcontrib/examples/satellites/data/satellites.csv')
    >> srf = SatRandomForest(sat_df)
    >> srf.probability('Intermediate', Perigee_km=535, Apogee_km=551,
            Eccentricity=0.00116, Period_minutes=95.5, Launch_Mass_kg=293,
            Power_watts=414,Anticipated_Lifetime=3, Class_of_Orbit='LEO'
            Purpose='Astrophysics', Users='Government/Civil')
    0.8499
    >> srf.simulate(10, Perigee_km=535, Apogee_km=551,
            Eccentricity=0.00116, Period_minutes=95.5, Launch_Mass_kg=293,
            Power_watts=414,Anticipated_Lifetime=3, Class_of_Orbit='LEO'
            Purpose='Astrophysics', Users='Government/Civil')
    ['Intermediate', 'Intermediate', 'Intermediate', 'Intermediate',
     'Intermediate', 'Intermediate', 'Intermediate', 'Sun-Synchronous',
     'Intermediate', 'Intermediate']


    Attributes
    ----------
    Please do not mess around with any (exploring is ok).

    Methods
    -------
    simulate(n_samples, **kwargs)
        Simulate Type_of_Orbit|kwargs.
    probability(target_val, **kwargs)
        Compute P(Type_of_Orbit|kwargs).
    """
    # Declare globals used by all instances of the class. Tried to write the
    # code such that these can be changed easily. These should be exposed
    # somehow
    features_numerical = [
        'Perigee_km', 'Apogee_km',
        'Eccentricity', 'Period_minutes', 'Launch_Mass_kg',
        'Power_watts', 'Anticipated_Lifetime']
    features_categorical = ['Class_of_Orbit', 'Purpose', 'Users']
    features = features_numerical + features_categorical
    target = ['Type_of_Orbit']


    def __init__(self, sat_df):
        """Initializes SatRandomForest forest using the pandas dataframe sat_df.
        The SRF is automatically trained.
        """
        # The dataset.
        self.dataset = pd.DataFrame()
        # Lookup for categoricals to code.
        self.lookup = dict()
        # Training set (regressors and labels)
        self.X_numerical = np.ndarray(0)
        self.X_categorical = np.ndarray(0)
        self.Y = np.ndarray(0)
        # Random Forests.
        self.rf_partial = RandomForestClassifier(n_estimators=100)
        self.rf_full = RandomForestClassifier(n_estimators=100)

        # Build the foreign predictor.
        self._create_dataset(sat_df)
        self._create_categorical_lookup()
        self._create_X_categorical()
        self._create_X_numerical()
        self._create_Y()
        self._train_rf()


    def _create_dataset(self, sat_df):
        """Create the dataframe of the satellites dataset. `NaN` strings are
        converted to Python `None`.
        Creates: self.dataset.
        """
        sat_df = sat_df.where((pd.notnull(sat_df)), None)
        self.dataset = sat_df[self.features + self.target].dropna(
            subset=self.target)

    def _create_categorical_lookup(self):
        """Builds a dictionary of dictionaries. Each dictionary contains the
        mapping category -> code for the corresponding categorical feature.
        Creates: self.lookup
        """
        for categorical in self.features_categorical:
            self.lookup[categorical] = {val:code for (code,val) in
                enumerate(self.dataset[categorical].unique())}

    def _create_X_categorical(self):
        """Converts each categorical column i (Ki categories, N rows) into an
        N x Ki matrix. Each row in the matrix is a binary vector.

        If there are J columns in features_categorical, then self.X_categorical
        will be N x (J*sum(Ki)) (ie all encoded categorical matrices are
        concatenated).

        Example

            Nationality  Gender
            -----------  ------
            USA          M
            France       F
            France       M
            Germany      M

            -----|-
            1 0 0 1
            0 1 0 0
            0 1 0 1
            0 0 1 1

        Creates: self.X_categorical
        """
        self.X_categorical = []
        for row in self.dataset.iterrows():
            data = list(row[1][self.features_categorical])
            binary_data = self._binarize_categorical_row(data)
            self.X_categorical.append(binary_data)
        self.x_categorical = np.asarray(self.X_categorical)

    def _binarize_categorical_row(self, data):
        """Unrolls a row of categorical data into the corresponding binary
        vector version. The order of the columns in `data` must be the same
        as self.features_categorical.
        The `data` must be a list of strings corresponding to the value
        of each categorical column.
        """
        assert len(data) == len(self.features_categorical)
        binary_data = []
        for categorical, value in zip(self.features_categorical, data):
            K = len(self.lookup[categorical])
            encoding = [0]*K
            encoding[self.lookup[categorical][value]] = 1
            binary_data.extend(encoding)
        return binary_data

    def _create_X_numerical(self):
        """Extract numerical columns from the dataset into a matrix.
        Creates: self.X_numerical
        """
        X_numerical = self.dataset[self.features_numerical].as_matrix().astype(
            float)
        # XXX This is necessary. sklearn cannot deal with missing values and
        # every row in the dataset has at least one missing value. The
        # imputer is generic. Cannot use CC since a foreign predictor
        # is independent.
        self.X_numerical = Imputer().fit_transform(X_numerical)

    def _create_Y(self):
        """Extracts the target column.
        Creates: self.Y
        """
        self.Y = self.dataset[self.target].as_matrix().ravel()

    def _train_rf(self):
        """Trains the random forests classifiers. We train two classifiers,
        `partial` which is just trained on `features_numerical`, and `full`
        which is trained on `features_numerical+features_categorical`.

        This feature is critical for safe querying; otherwise sklearn would
        crash whenever a categorical value unseen in training due to filtering
        (but existant in sat_df nevertheless) was passed in.

        `Full` and `partial` have been shown to produce comparable results, but
        `full` is invariably of higher quality.
        """
        self.rf_partial.fit_transform(self.X_numerical, self.Y)
        self.rf_full.fit_transform(
            np.hstack((self.X_numerical, self.X_categorical)), self.Y)

    def _compute_target_distribution(self, **kwargs):
        """Given kwargs, which is a dict of {feature_col:val}, returns the
        distribution and (label mapping for lookup) of the random label:
            self.target|kwargs
        """
        if set(kwargs.keys()) != set(self.features_numerical +
                self.features_categorical):
            raise ValueError('Must specify values for all the conditionals.\n'
                'Received: {}\n'
                'Expected: {}'.format(kwargs, self.features_numerical +
                self.features_categorical))

        # Are there any category values in kwargs which never appeared during
        # training? If yes, we need to run the partial RF.
        unseen = any([kwargs[cat] not in self.lookup[cat]
            for cat in self.features_categorical])

        X_numerical = [kwargs[col] for col in self.features_numerical]

        if unseen:
            distribution = self.rf_partial.predict_proba(X_numerical)
            classes = self.rf_partial.classes_
        else:
            X_categorical = [kwargs[col] for col in self.features_categorical]
            X_categorical = self._binarize_categorical_row(X_categorical)
            distribution = self.rf_full.predict_proba(
                np.hstack((X_numerical,X_categorical)))
            classes = self.rf_partial.classes_

        return distribution[0], classes


    def simulate(self, n_samples, **kwargs):
        """Simulates n_samples of target|kwargs from the distribution learned
        by the SatRandomForest.
        kwargs must be of the form feature_col=val.
        """
        distribution, classes = self._compute_target_distribution(**kwargs)
        simulated = np.random.multinomial(1, distribution, size=n_samples)
        return [classes[np.where(s==1)[0][0]] for s in simulated]

    def probability(self, target_val, **kwargs):
        """Computes the probability P(target=target_val|kwargs) under
        distribution learned by the RandomForest.
        kwargs must be of the form feature_col=val.
        """
        distribution, classes = self._compute_target_distribution(**kwargs)
        if target_val not in classes:
            return 0
        return distribution[np.where(classes==target_val)[0][0]]
