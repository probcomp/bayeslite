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


# UNION OF CONCERNED SCIENTISTS SATELLITES DATABASE
# =================================================
# The Union of Concerned Scientists (UCS) created a database of characteristics
# of over 1000 orbiting satellites launched by countries around the world for
# any number of purposes and user categories, providing a new opportunity to
# analyze exactly what's going on above our heads. With the data set in hand,
# we're interested in seeing what BayesDB can detect about relationships in the
# data.
#   You can find the latest version of the data here:
# http://www.ucsusa.org/nuclear_weapons_and_global_security/solutions/space-weapons/ucs-satellite-database.html#.VTptWa1VhBc).

# NOTE: The output on your machine is expected to be similar to, but not
# exactly the same as the output pasted intot this file.

import bayeslite
import bayeslite.crosscat
from bayeslite import core

from crosscat.MultiprocessingEngine import MultiprocessingEngine
from bayeslite.shell.pretty import pp_cursor

import os
import sys


def pprint(cursor):
    return pp_cursor(sys.stdout, cursor)


def do_query(bdb, bql, bindings=None):
    if bindings is None:
        bindings = ()
    print '--> ' + bql.lstrip()
    return bdb.execute(bql, bindings)


btable = 'satellites'
generator = 'satellites_cc'
csv_filename = os.path.join('data', 'satellites.csv')
codebook_filename = os.path.join('data', 'satellites_codebook.csv')

# CREATING A BAYESDB INSTANCE
# ---------------------------
# We will first spin up a BayesDB instance which creates a SQLite3 database. In
# this case, we will not supply a filename to `bayeslite.BayesDB` so that the
# database is created in memory and no files are written to disk.

bdb = bayeslite.bayesdb_open()

# LOADING IN DATA
# ---------------
# Next, we create a btable by loading the data from a .csv file. We let BayesDB
# know that we have supplied a file in which the first row is a header
# specifying the column names. **Note**: missing data should be left blank in
# the csv file.
#   We also supply a codebook for our data. The codebook is a .csv file that
# contains extra information about the columns, which makes the data more
# interpretable. The codebook has four columns:
#     - `name`: the column name to which the metadata are applied
#     - `shortname`: a short, but human-readable column name
#     - `description`: a long description of the column
#     - `value_map`: a map of csv values to data values in JSON format. E.g.
#       `{"0": "False", "1": "True"}`. Note that you will need to quote your
#       JSON in your codebook csv.

bayeslite.bayesdb_read_csv_file(bdb, btable, csv_filename, header=True,
                                create=True)
bayeslite.bayesdb_load_codebook_csv_file(bdb, btable, codebook_filename)

# SPECIFYING A METAMODEL AND CREATING A GENERATOR
# -----------------------------------------------
# Next, we must specify the statistical metamodel by which we shall model the
# data in our btable. Currently BayesDB supports only the CrossCat metamodel
# (for more on CrossCat, visit the repo). We must let BayesDB know that we want
# to model our btable with CrossCat and that we want to use CrossCat's
# multiprocessing engine.

engine = bayeslite.crosscat.CrosscatMetamodel(MultiprocessingEngine())
bayeslite.bayesdb_register_metamodel(bdb, engine)

# CREATING A GENERATOR
# --------------------
# A generator is an instance of a metamodel that is linked to a specific
# btable. The generator is the workhorse of BayesDB. Through it, we do pretty
# much everything that goes beyong regular SQL.
#   Let's create a generator and tell it which stastistical types to assign to
# each column.

bql = '''
CREATE GENERATOR satellites_cc FOR satellites
    USING crosscat (
        GUESS(*),
        Name IGNORE,
        longitude_radians_of_geo CYCLIC,
        Inclination_radians CYCLIC
    );
'''
c = do_query(bdb, bql)

# We've created a generator called `satellites_cc` for the btable `satellites`
# using the crosscat metamodel we specified earlier. We've told BayesDB to
# model the columns `longitude_radians_of_geo` and `Inclination_radians` as
# cyclic variables, to ignore the `Name` column (because each value is unique
# and categorical and would slow down analyses while adding no quality), and
# to `GUESS` the remaining types. BayesDB does an OK job at guessing your
# types, but you can (and should!) verify that the statistical types are
# correct. CrossCat supports `CATEGORICAL`, `NUMERICAL`, and `CYCLIC`
# statistical types.
#
# VERIFY STATISTICAL TYPES
# ------------------------
# The statistical types of the columns can be retrieved from the metadata
# tables, `bayesdb_generator` and `bayesdb_generator_column`.

generator_id = core.bayesdb_get_generator(bdb, generator)
sql = '''
SELECT c.colno AS colno, c.name AS name,
        gc.stattype AS stattype, c.shortname AS shortname
    FROM bayesdb_generator AS g,
        (bayesdb_column AS c LEFT OUTER JOIN
            bayesdb_generator_column AS gc
            USING (colno))
    WHERE g.id = ? AND g.id = gc.generator_id;
'''
c = bdb.sql_execute(sql, (generator_id,))
pprint(c)

# colno |                         name |    stattype |          shortname
# ------+------------------------------+-------------+-------------------
#     1 |          Country_of_Operator | categorical |            Country
#     2 |               Operator_Owner | categorical |     Operator/owner
#     3 |                        Users | categorical |              Users
#     4 |                      Purpose | categorical |            Purpose
#     5 |               Class_of_Orbit | categorical |     class of orbit
#     6 |                Type_of_Orbit | categorical |         Orbit type
#     7 |                   Perigee_km |   numerical |            Perigee
#     8 |                    Apogee_km |   numerical |             Apogee
#     9 |                 Eccentricity |   numerical |       Eccentricity
#    10 |               Period_minutes |   numerical |             Period
#    11 |               Launch_Mass_kg |   numerical |        Launch mass
#    12 |                  Dry_Mass_kg |   numerical |           Dry mass
#    13 |                  Power_watts |   numerical |              Power
#    14 |               Date_of_Launch |   numerical |        Launch date
#    15 |            Expected_Lifetime |   numerical |  Expected lifetime
#    16 |                   Contractor | categorical |         Contractor
#    17 |        Country_of_Contractor | categorical | Contractor country
#    18 |                  Launch_Site | categorical |        Launch site
#    19 |               Launch_Vehicle | categorical |     Launch vehicle
#    20 | Source_Used_for_Orbital_Data | categorical |  Orbit data source
#    21 |     longitude_radians_of_geo |      cyclic |          Longitude
#    22 |          Inclination_radians |      cyclic |        Inclination

# BayesDB did a good job of guessing the statistical types of the columns we
# didn't explicitly specify. Now we are ready to run begin analyzing our data.

# RUNNING ANALYSES
# ----------------
# We use BayesDB to generate independent samples, models, from CrossCat. To do
# this, we `INITIALIZE` some number of models (from the prior) and run the
# sampling algorithm for some duration.
#   For purposes of expedience, we'll analyze a small number of models for a
# short amount of time.

c = do_query(bdb, 'INITIALIZE 8 MODELS FOR satellites_cc;')
c = do_query(bdb, 'ANALYZE satellites_cc FOR 2 MINUTES WAIT;')

# We have asked BayesDB for initalize 8 models and to run the sampling
# algorithm for 2 minutes. The WAIT keyword tells BayesDB "we are not
# interested in any metadata untill 2 minutes are over, so just keep running."
# In practice we'd want far more models with far more iterations, but these
# meager analyses are sufficent for pedagoical purposes.
#   We can see how many iterations we were able to get in two minutes by
# acessing the bayesdb_generator_model table.

bql = '''
SELECT modelno, iterations FROM bayesdb_generator_model
    WHERE generator_id = ?
    ORDER BY modelno ASC;
'''
c = do_query(bdb, bql, (generator_id,))
pprint(c)

# modelno | iterations
# --------+-----------
#       0 |         96
#       1 |         96
#       2 |         96
#       3 |         96
#       4 |         96
#       5 |         96
#       6 |         96
#       7 |         96

# FINDING ANOMALIES
# -----------------
# Let's ask BayesDB which satellites have suprising expected lifetimes. We'll
# do this by asking BayesDB for the probability of the obsered values for
# Expected_Lifetime under the model it has inferred. Lower probability is
# considered to be more suprising. Note that predictive probabilites are PDF
# or PMF values; do not be alarmed if you find that predictive probability of
# a NUMERICAL or CYCLIC variable is greater than 1.
#    Because calculating PREDICTIVE PROBABILITY over an entire column is
# expensive, we shall cache this computation using SQL temporary tables. We
# shall then print out the 20 most anomalous entries---and let's use some of
# SQLite3's built in fucntions to make sure that the output fits on the screen
# (some of these satellite names are long).

bql = '''
CREATE TEMP TABLE predprob_life AS
    ESTIMATE Name, Expected_Lifetime,
         PREDICTIVE PROBABILITY OF Expected_Lifetime AS p_lifetime,
         Class_of_Orbit
    FROM satellites_cc;
'''
c = do_query(bdb, bql)

bql = '''
SELECT substr(Name, 1, 27) as Name, Class_of_Orbit, Expected_Lifetime,
        p_lifetime
    FROM predprob_life
    ORDER BY p_lifetime ASC
    LIMIT 20;
'''
c = do_query(bdb, bql)
pprint(c)

#                        Name | Class_of_Orbit | Expected_Lifetime |        p_lifetime
# ----------------------------+----------------+-------------------+------------------
# International Space Station |            LEO |                30 | 1.56209529169e-08
# Milstar DFS-5 (USA 164, Mil |            GEO |                 0 |  0.00119054844273
#                   Landsat 7 |            LEO |                15 |  0.00120818968604
# DSP 21 (USA 159) (Defense S |            GEO |               0.5 |  0.00173913111633
# DSP 22 (USA 176) (Defense S |            GEO |               0.5 |  0.00173913111633
# Express-A1R (Express 4A, Ek |            GEO |               0.5 |  0.00173913111633
#                      GSAT-2 |            GEO |               0.5 |  0.00173913111633
#                Intelsat 701 |            GEO |               0.5 |  0.00173913111633
#        Kalpana-1 (Metsat-1) |            GEO |               0.5 |  0.00173913111633
#                    Optus B3 |            GEO |               0.5 |  0.00173913111633
# SDS III-3 (Satellite Data S |            GEO |               0.5 |  0.00173913111633
#                   Sicral 1A |            GEO |               0.5 |  0.00173913111633
#                   Sicral 1B |            GEO |               0.5 |  0.00173913111633
# MUOS-1 (Mobile User Objecti |            GEO |               0.5 |  0.00429299588829
# MUOS-2 (Mobile User Objecti |            GEO |               0.5 |  0.00429299588829
# Globalstar M073 (Globalstar |            LEO |                15 |  0.00685690228626
# Globalstar M074 (Globalstar |            LEO |                15 |  0.00685690228626
# Globalstar M075 (Globalstar |            LEO |                15 |  0.00685690228626
# Globalstar M076 (Globalstar |            LEO |                15 |  0.00685690228626
# Globalstar M077 (Globalstar |            LEO |                15 |  0.00685690228626

# We see that the International Space Station has, by far, the most suprising
# lifetime. The ISS is, of course, one of only a couple of satellites that we
# go up and fix! BayesDB also found Milstar DFS-5's zero expected lifetime
# suprising. Perhaps this is a data entry error, which brings us to...

# FINDING DATA ENTRY ERRORS
# -------------------------
# Many of the columns in this table share nearly deterministic relationships
# governed by the laws of physics, for example, the relationship between type
# of orbit and period. Searching for anomalies in these columns may reveal data
# entry errors. We shall proceed as before.

bql = '''
CREATE TEMP TABLE predprob_period AS
    ESTIMATE Name, class_of_orbit, Period_minutes,
        PREDICTIVE PROBABILITY OF Period_minutes AS p_period
    FROM satellites_cc;
'''
c = do_query(bdb, bql)

bql = '''
SELECT substr(Name, 1, 23) as Name, Class_of_Orbit, Period_minutes, p_period
    FROM predprob_period
    ORDER BY p_period ASC
    LIMIT 10;
'''
c = do_query(bdb, bql)
pprint(c)

#                        Name | Class_of_Orbit | Period_minutes |          p_period
# ----------------------------+----------------+----------------+------------------
# SDS III-6 (Satellite Data S |            GEO |          14.36 | 4.85458199304e-06
# Advanced Orion 6 (NRO L-15, |            GEO |          23.94 | 5.42967058743e-06
# SDS III-7 (Satellite Data S |            GEO |          23.94 | 5.42967058743e-06
# DSP 20 (USA 149) (Defense S |            GEO |         142.08 | 1.24610616899e-05
# Glonass 733 (Glonass 41-2,  |            MEO |         670.47 | 0.000605915021079
# Glonass 736 (Glonass 43-1,  |            MEO |          671.2 | 0.000609617409446
# Glonass 729 (Glonass 39-3,  |            MEO |         671.71 | 0.000612199551074
# Glonass 722 (Glonass 37-2,  |            MEO |          672.7 | 0.000617200891068
# Glonass 734 (Glonass 41-3,  |            MEO |         675.08 | 0.000629159110986
# Glonass 724 (Glonass 38-1,  |            MEO |          675.2 | 0.000629759421569

# Indeed, we have found several non-physical entries. A geosynchronous orbit
# should have a period close to 24 hours, or 1440 minutes. SDS III-6 and DSP 20
# appear to have decimal place errors, while Advanced Orion 6 and SDS III-7
# appear to have entries in hours, not minutes.
#   **Note**: The 2/1/15 UCS data do not have the Advanced Orion 6 error and we
# have notified the UCS of the other errors (we're not jerks!).
#
# THE STRENGTH OF DEPENDENCIES
# ----------------------------
# To see which variables are dependent with period, we ask BayesDB about the
# dependence probability of period with the other columns.

bql = '''
ESTIMATE COLUMNS DEPENDENCE PROBABILITY WITH Period_minutes as depprob_period
    FROM satellites_cc
    ORDER BY depprob_period DESC LIMIT 10;
'''
c = do_query(bdb, bql)
pprint(c)

#                         name | depprob_period
# -----------------------------+---------------
#               Class_of_Orbit |            1.0
#                Type_of_Orbit |            1.0
#                   Perigee_km |            1.0
#                    Apogee_km |            1.0
#               Period_minutes |              1
#            Expected_Lifetime |            1.0
# Source_Used_for_Orbital_Data |            1.0
#          Inclination_radians |            1.0
#                 Eccentricity |           0.75
#                        Users |          0.625

# BayesDB infers that columns that are physically dependent are highly
# statistically dependent.
#
# SIMULATING DATA
# ---------------
# We obtain hypotheical data from BayesDB by creating temporary tables and
# filling them with SIMULATE. We can specify conditions using GIVEN and specify
# how many samples we want using LIMIT.

bql = '''
CREATE TEMP TABLE simusers20 AS
    SIMULATE users FROM satellites_cc GIVEN expected_lifetime = 20
    LIMIT 100;
'''
c = do_query(bdb, bql)

bql = '''
SELECT users, COUNT(*) AS total
    FROM simusers20
    GROUP BY users
    ORDER BY TOTAL DESC;
'''
c = do_query(bdb, bql)
pprint(c)

#                 users | total
# ----------------------+------
#            Commercial |    30
#            Government |    27
#              Military |    15
#                 Civil |     8
#   Military/Commercial |     8
#      Government/Civil |     3
# Government/Commercial |     3
#   Government/Military |     2
#   Military/Government |     2
#        Civil/Military |     1
#             Commecial |     1

# FURTHER READING
# ===============
# The API documentation can be found here:
# FIXME
#
# Shell-based examples can be found in the contrib repository:
# https://github.com/mit-probabilistic-computing-project/bdbcontrib
