import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import bayeslite
import bdbcontrib
import os

# so we can build bdb models
os.environ['BAYESDB_WIZARD_MODE']='1'

out_dir = 'output'

csv_file = 'satellites.csv'
bdb_file = out_dir + '/satellites.bdb'
num_models = 64
time_minutes = 60

if not os.path.isdir(out_dir):
    os.makedirs(out_dir)
if os.path.exists(bdb_file):
    print 'Error. file', bdb_file, 'already exists. Please remove it.'
    sys.exit(1)

# create database mapped to filesystem
print 'opening bdb on disk:', bdb_file
bdb = bayeslite.bayesdb_open(pathname=bdb_file, builtin_metamodels=False)

# read csv into table
bayeslite.bayesdb_read_csv_file(bdb, "satellites", csv_file,
        header=True, create=True, ifnotexists=True)

# register crosscat metamodel
import crosscat.MultiprocessingEngine as ccme
import bayeslite.metamodels.crosscat
cc = ccme.MultiprocessingEngine(seed=0, cpu_count=num_models)
ccmm = bayeslite.metamodels.crosscat.CrosscatMetamodel(cc)
bayeslite.bayesdb_register_metamodel(bdb, ccmm)

# create the crosscat generator using
bdb.execute('''
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

cmd = 'initialize %d models for satellites_cc' % (num_models,)
print cmd
bdb.execute(cmd)

cmd = 'analyze satellites_cc for %d minutes checkpoint 60 seconds wait' % time_minutes
print cmd
bdb.execute(cmd)

# create a diagnostics plot
#fig = bdbcontrib.plot_crosscat_chain_diagnostics(bdb, 'logscore',
    #'satellites_cc')
#plt.savefig('output/satellites_logscores.pdf')

print 'closing bdb', bdb_file
bdb.close()
