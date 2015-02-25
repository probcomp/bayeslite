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

import StringIO
import cmd
import traceback

import bayeslite
import bayeslite.core as core
import bayeslite.parse as parse
import bayeslite.shell.pretty as pretty

from bayeslite.util import casefold

class MockCursor(object):
    def __init__(self, description, rows):
        self.description = description
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

class Shell(cmd.Cmd):
    def_prompt          = 'bayeslite> '
    bql_prompt          = '   bql...> '
    sql_prompt          = '   sql...> '
    python_prompt       = 'python...> '

    def __init__(self, bdb):
        self.bdb = bdb
        self.prompt = self.def_prompt
        self.bql = StringIO.StringIO()
        cmd.Cmd.__init__(self, 'Tab')

    def cmdloop(self, *args, **kwargs):
        while True:
            try:
                cmd.Cmd.cmdloop(self, *args, **kwargs)
            except KeyboardInterrupt:
                self.stdout.write('^C\n')
                continue
            else:
                break

    def default(self, line):
        # XXX What is this idiocy?  End-of-input is reported the same
        # as the line with characters `E', `O', `F'.
        if line == 'EOF':
            self.stdout.write('\nMoriturus te querio.\n')
            return True
        # Add a line and check whether it finishes a BQL phrase.
        self.bql.write(line)
        self.bql.write('\n')
        bql = self.bql.getvalue()
        if parse.bql_string_complete_p(bql):
            # Reset the BQL input.
            self.bql = StringIO.StringIO()
            self.prompt = self.def_prompt
            try:
                with self.bdb.savepoint():
                    cursor = self.bdb.execute(bql)
                    pretty.pp_cursor(self.stdout, cursor)
            except Exception:
                self.stdout.write(traceback.format_exc())
        else:
            self.prompt = self.bql_prompt
        return False

    def do_sql(self, line):
        try:
            pretty.pp_cursor(self.stdout, self.bdb.sql_execute(line))
        except Exception:
            self.stdout.write(traceback.format_exc())
        return False

    def do_python(self, line):
        try:
            self.stdout.write('%s\n' % (repr(eval(line)),))
        except Exception:
            self.stdout.write(traceback.format_exc())
        return False

    def trace(self, q, b):
        self.stdout.write('--> %s %s\n' % (q.strip(), b))

    def sql_trace(self, q, b):
        self.stdout.write('==> %s %s\n' % (q.strip(), b))

    def do_trace(self, line):
        if line == 'bql':
            self.bdb.trace(self.trace)
        elif line == 'sql':
            self.bdb.sql_trace(self.sql_trace)
        else:
            self.stdout.write('Trace what?\n')

    def do_untrace(self, line):
        if line == 'bql':
            self.bdb.untrace(self.trace)
        elif line == 'sql':
            self.bdb.sql_untrace(self.sql_trace)
        else:
            self.stdout.write('Untrace what?\n')

    def do_csv(self, line):
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 2:
            self.stdout.write('Usage: import <table> </path/to/data.csv>\n')
        table = tokens[0]
        pathname = tokens[1]
        try:
            bayeslite.bayesdb_import_csv_file(self.bdb, table, pathname)
        except Exception:
            self.stdout.write(traceback.format_exc())

    def do_codebook(self, line):
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 2:
            self.stdout.write('Usage:'
                ' codebook <table> </path/to/codebook.csv>\n')
        table = tokens[0]
        pathname = tokens[1]
        bayeslite.bayesdb_import_codebook_csv_file(self.bdb, table, pathname)

    def do_loadmodels(self, line):
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 2:
            self.stdout.write('Usage:'
                ' loadmodels <table> </path/to/models.pkl.gz>\n')
        table = tokens[0]
        pathname = tokens[1]
        bayeslite.bayesdb_load_legacy_models(self.bdb, table, pathname)

    def do_describe(self, line):
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) == 0:
            self.stdout.write('Describe what, pray tell?\n')
        elif casefold(tokens[0]) == 'btable' or \
             casefold(tokens[0]) == 'btables':
            params = None
            qualifier = None
            if len(tokens) == 1:
                params = ()
                qualifier = '1'
            else:
                params = tokens[1:]
                qualifier = \
                    '(' + ' OR '.join(['t.name = ?' for _p in params]) + ')'
            sql = '''
                SELECT t.id, t.name, m.name AS metamodel
                    FROM bayesdb_table AS t, bayesdb_metamodel AS m
                    WHERE %s AND t.metamodel_id = m.id
            ''' % (qualifier,)
            with self.bdb.savepoint():
                pretty.pp_cursor(self.stdout, self.bdb.execute(sql, params))
        elif casefold(tokens[0]) == 'columns':
            if len(tokens) != 2:
                self.stdout.write('Describe columns of what btable?\n')
                return
            table_name = tokens[1]
            with self.bdb.savepoint():
                if not core.bayesdb_table_exists(self.bdb, table_name):
                    self.stdout.write('No such btable: %s\n' % (table_name,))
                    return
                table_id = core.bayesdb_table_id(self.bdb, table_name)
                M_c = core.bayesdb_metadata(self.bdb, table_id)
                sql = '''
                    SELECT c.colno, c.name, c.short_name
                        FROM bayesdb_table AS t, bayesdb_table_column AS c
                        WHERE t.id = ? AND c.table_id = t.id
                '''
                params = (table_id,)
                cursor0 = self.bdb.sql_execute(sql, params)
                description = (
                    ('colno',),
                    ('name',),
                    ('model type',),
                    ('short name',),
                )
                rows = ((colno, name,
                         M_c['column_metadata'][colno]['modeltype'],
                         short_name)
                    for colno, name, short_name in cursor0)
                cursor1 = MockCursor(description, rows)
                pretty.pp_cursor(self.stdout, cursor1)
        elif casefold(tokens[0]) == 'models':
            if len(tokens) < 2:
                self.stdout.write('Describe models of what btable?\n')
                return
            table_name = tokens[1]
            with self.bdb.savepoint():
                if not core.bayesdb_table_exists(self.bdb, table_name):
                    self.stdout.write('No such btable: %s\n', table_name)
                    return
                table_id = core.bayesdb_table_id(self.bdb, table_name)
                modelnos = None
                if len(tokens) == 2:
                    sql = '''
                        SELECT modelno FROM bayesdb_model WHERE table_id = ?
                    '''
                    cursor = self.bdb.sql_execute(sql, (table_id,))
                    modelnos = (row[0] for row in cursor)
                else:
                    for token in tokens[2:]:
                        try:
                            modelno = int(token)
                        except ValueError:
                            self.stdout.write('Invalid model number: %s\n' %
                                (token,))
                            return
                        else:
                            if not core.bayesdb_has_model(self.bdb, table_id,
                                    modelno):
                                self.stdout.write('No such model: %d\n' %
                                    (modelno,))
                                return
                    modelnos = map(int, tokens[2:])
                models = ((modelno, core.bayesdb_model(self.bdb, table_id,
                        modelno))
                    for modelno in modelnos)
                cursor = MockCursor([('modelno',), ('iterations',)],
                    [(modelno, theta['iterations'])
                        for modelno, theta in models])
                pretty.pp_cursor(self.stdout, cursor)
        else:
            self.stdout.write('I don\'t know what a %s is.\n' %
                (repr(tokens[0]),))
