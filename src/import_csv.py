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

import csv
import math

import bayeslite.core as core
import bayeslite.imp as imp

# XXX Allow the user to pass in the desired encoding (and CSV dialect,
# &c.).
def bayesdb_read_csv_with_header(pathname):
    with open(pathname, "rU") as f:
        reader = csv.reader(f)
        try:
            header = reader.next()
        except StopIteration:
            raise IOError("Empty CSV file")
        # XXX Let the user pass in the desired encoding.
        column_names = [unicode(n, "utf8").strip() for n in header]
        ncols = len(column_names)
        if ncols == 0:
            raise IOError("No columns in CSV file!")
        # XXX Can we get the CSV reader to decode and strip for us?
        rows = [[unicode(v, "utf8").strip() for v in row] for row in reader]
        for row in rows:
            if len(row) != ncols:
                raise IOError("Mismatched number of columns")
        return column_names, rows

def bayesdb_import_csv_file(bdb, table, pathname, column_types=None,
        ifnotexists=False):
    def generator():
        return bayesdb_read_csv_with_header(pathname)
    imp.bayesdb_import_generated(bdb, table, generator, column_types,
        ifnotexists)
