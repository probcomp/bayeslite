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
import json

import bayeslite.core as core

def bayesdb_import_codebook_csv_file(bdb, table_name, pathname):
    codebook = None
    with open(pathname, 'rU') as f:
        reader = csv.reader(f)
        try:
            header = reader.next()
        except StopIteration:
            raise IOError('Empty codebook file')
        header = [unicode(h, 'utf8').strip() for h in header]
        if header != ['column_label','short_name','description','value_map']:
            raise IOError('Wrong CSV header for codebook')
        codebook = []
        line = 1
        for row in reader:
            if len(row) != 4:
                raise IOError('Wrong number of columns at line %d: %d' %
                    (line, len(row)))
            column_name, _short_name, _description, _value_map_json = row
            table_id = core.bayesdb_table_id(bdb, table_name)
            codebook.append(row)
            line += 1
    with bdb.savepoint():
        table_id = core.bayesdb_table_id(bdb, table_name)
        for column_name, short_name, description, value_map_json in codebook:
            if not core.bayesdb_column_exists(bdb, table_id, column_name):
                raise IOError('Column does not exist in table %s: %s' %
                    (repr(table_name), repr(column_name)))
            colno = core.bayesdb_column_number(bdb, table_id, column_name)
            if value_map_json != 'NaN':      # ...
                value_map = json.loads(value_map_json)
                sql = '''
                    DELETE FROM bayesdb_value_map
                        WHERE table_id = ? AND colno = ?
                '''
                bdb.sql_execute(sql, (table_id, colno))
                sql = '''
                    INSERT INTO bayesdb_value_map
                        (table_id, colno, value, extended_value)
                        VALUES (?, ?, ?, ?)
                '''
                for v in sorted(value_map.keys()):
                    bdb.sql_execute(sql, (table_id, colno, v, value_map[v]))
            sql = '''
                UPDATE bayesdb_table_column
                    SET short_name = ?, description = ?
                    WHERE table_id = ? AND colno = ?
            '''
            bdb.sql_execute(sql, (short_name, description, table_id, colno))
