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

import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import bayeslite
import bdbcontrib
import os
import time
import datetime
import subprocess

then = time.time()

out_dir = 'output'

timestamp = datetime.datetime.fromtimestamp(then).strftime('%Y-%m-%d')
user = subprocess.check_output(["whoami"]).strip()
host = subprocess.check_output(["hostname"]).strip()
filestamp = '-' + timestamp + '-' + user
def out_file_name(base, ext):
    return out_dir + '/' + base + filestamp + ext

num_models = 64
num_iters = 16 * 15 # Expect this to run for ~15 minutes on probcomp
seed = 0

csv_file = 'satellites.csv'
bdb_file = out_file_name('satellites', '.bdb')

# so we can build bdb models
os.environ['BAYESDB_WIZARD_MODE']='1'

if not os.path.isdir(out_dir):
    os.makedirs(out_dir)
if os.path.exists(bdb_file):
    print 'Error: File', bdb_file, 'already exists. Please remove it.'
    sys.exit(1)

def log(msg):
    print "At %3.2fs" % (time.time() - then), msg

# create database mapped to filesystem
log('opening bdb on disk: %s' % bdb_file)
bdb = bayeslite.bayesdb_open(pathname=bdb_file, builtin_metamodels=False)

def execute(bql):
    log("executing %s" % bql)
    bdb.execute(bql)

# read csv into table
log('reading data from %s' % csv_file)
bayeslite.bayesdb_read_csv_file(bdb, 'satellites', csv_file,
        header=True, create=True, ifnotexists=True)

# Add a "not applicable" orbit sub-type
log('adding "not applicable" orbit sub-type')
bdb.sql_execute('''UPDATE satellites
    SET type_of_orbit = 'N/A'
    WHERE (class_of_orbit = 'GEO' OR class_of_orbit = 'MEO')
      AND type_of_orbit = 'NaN'
''')

# nullify "NaN"
log('nullifying NaN')
bdbcontrib.nullify(bdb, 'satellites', 'NaN')

# register crosscat metamodel
import crosscat
import crosscat.MultiprocessingEngine as ccme
import bayeslite.metamodels.crosscat
cc = ccme.MultiprocessingEngine(seed=seed, cpu_count=num_models)
ccmm = bayeslite.metamodels.crosscat.CrosscatMetamodel(cc)
bayeslite.bayesdb_register_metamodel(bdb, ccmm)

# create the crosscat generator using
execute('''
    CREATE GENERATOR satellites_cc FOR satellites USING crosscat (
        GUESS(*),
        name IGNORE,
        Country_of_Operator CATEGORICAL,
        Operator_Owner CATEGORICAL,
        Users CATEGORICAL,
        Purpose CATEGORICAL,
        Class_of_Orbit CATEGORICAL,
        Type_of_Orbit CATEGORICAL,
        Perigee_km NUMERICAL,
        Apogee_km NUMERICAL,
        Eccentricity NUMERICAL,
        Period_minutes NUMERICAL,
        Launch_Mass_kg NUMERICAL,
        Dry_Mass_kg NUMERICAL,
        Power_watts NUMERICAL,
        Date_of_Launch NUMERICAL,
        Anticipated_Lifetime NUMERICAL,
        Contractor CATEGORICAL,
        Country_of_Contractor CATEGORICAL,
        Launch_Site CATEGORICAL,
        Launch_Vehicle CATEGORICAL,
        Source_Used_for_Orbital_Data CATEGORICAL,
        longitude_radians_of_geo NUMERICAL,
        Inclination_radians NUMERICAL
    )
''')

execute('INITIALIZE %d MODELS FOR satellites_cc' % (num_models,))

execute('ANALYZE satellites_cc FOR %d ITERATIONS CHECKPOINT 5 ITERATIONS WAIT'
        % num_iters)

# create a diagnostics plot
plot_file_name = out_file_name('satellites', '-logscores.pdf')
log('writing diagnostic plot to %s' % plot_file_name)
fig = bdbcontrib.plot_crosscat_chain_diagnostics(bdb, 'logscore',
    'satellites_cc')
plt.savefig(plot_file_name)

log('closing bdb %s' % bdb_file)
bdb.close()

metadata_file = out_file_name('satellites', '-meta.txt')
sha_sum = subprocess.check_output(["sha256sum", bdb_file])
total_time = time.time() - then
def record_metadata(f):
    f.write("DB file " + bdb_file + "\n")
    f.write(sha_sum)
    f.write("built from " + csv_file + "\n")
    f.write("by %s@%s\n" % (user, host))
    f.write("at seed %s\n" % seed)
    f.write("in %3.2f seconds\n" % total_time)
    f.write("with %s models analyzed for %s iterations\n"
            % (num_models, num_iters))
    f.write("by bayeslite %s, with crosscat %s and bdbcontrib %s\n"
            % (bayeslite.__version__, crosscat.__version__, bdbcontrib.__version__))
    f.write("diagnostics recorded to %s\n" % plot_file_name)
    f.flush()

with open(metadata_file, 'w') as fd:
    record_metadata(fd)
    fd.write('using script ')
    fd.write('-' * 57)
    fd.write('\n')
    fd.flush()
    os.system("cat %s >> %s" % (__file__, metadata_file))

record_metadata(sys.stdout)
