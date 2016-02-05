# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
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
import apsw
import cmd
import traceback
import sys

import bayeslite
import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.guess as guess
import bayeslite.parse as parse
import bayeslite.shell.pretty as pretty
import bayeslite.txn as txn

from bayeslite.util import casefold


class Shell(cmd.Cmd):
    def_prompt    = 'bayeslite> '
    bql_prompt    = '   bql...> '
    sql_prompt    = '   sql...> '
    python_prompt = 'python...> '

    def __init__(self, bdb, metamodel, stdin=None, stdout=None, stderr=None):
        if stdin is None:
            stdin = sys.stdin
        if stdout is None:
            stdout = sys.stdout
        if stderr is None:
            stderr = sys.stderr
        self.prompt = self.def_prompt
        self.bql = StringIO.StringIO()
        self.identchars += '.'
        self.stderr = stderr
        cmd.Cmd.__init__(self, 'Tab', stdin, stdout)

        self._bdb = bdb
        self._metamodel = metamodel
        self._cmds = set([])
        self._traced = False
        self._sql_traced = False
        self._hooked_filenames = set([])

        self._python_globals = {'bayeslite': bayeslite}

        # Awful kludge to make commands begin with `.'.
        #
        # XXX Does not disable the `quit' command and whatever other
        # bollocks is built-in.
        self._installcmd('codebook', self.dot_codebook)
        self._installcmd('csv', self.dot_csv)
        self._installcmd('describe', self.dot_describe)
        self._installcmd('guess', self.dot_guess)
        self._installcmd('help', self.dot_help)
        self._installcmd('hook', self.dot_hook)
        self._installcmd('legacymodels', self.dot_legacymodels)
        self._installcmd('open', self.dot_open)
        self._installcmd('pythexec', self.dot_pythexec)
        self._installcmd('python', self.dot_python)
        self._installcmd('read', self.dot_read)
        self._installcmd('sql', self.dot_sql)
        self._installcmd('trace', self.dot_trace)
        self._installcmd('untrace', self.dot_untrace)

        self._core_commands = set(self._cmds)

        self.hookvars = {}

    def _installcmd(self, name, method):
        assert not hasattr(self, 'do_.%s' % (name,))
        assert name not in self._cmds
        setattr(self, 'do_.%s' % (name,), method)
        self._cmds.add(name)

    def _uninstallcmd(self, name):
        if name in self._core_commands:
            raise ValueError('Cannot uninstall core command: %s' % (name,))
        delattr(self, 'do_.%s' % (name,))
        self._cmds.remove(name)

    def cmdloop(self, *args, **kwargs):
        version = bayeslite.__version__
        self.stdout.write('Welcome to the Bayeslite %s shell.\n' % (version,))
        self.stdout.write('Type `.help\' for help.\n')
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
        if self.prompt == self.def_prompt:
            if line.startswith('.'):
                cmd = line
                for i, c in enumerate(line):
                    if c in (' ', '\t'):
                        cmd = line[:i]
                        break
                self.stdout.write('Unknown command: %s\n' % (cmd,))
                return False
        # Add a line and check whether it finishes a BQL phrase.
        self.bql.write(line)
        self.bql.write('\n')
        string = self.bql.getvalue()
        if parse.bql_string_complete_p(string):
            # Reset the BQL input.
            self.bql = StringIO.StringIO()
            self.prompt = self.def_prompt
            try:
                first = True
                for phrase in parse.parse_bql_string(string):
                    cursor = bql.execute_phrase(self._bdb, phrase)
                    with txn.bayesdb_caching(self._bdb):
                        # Separate the output tables by a blank line.
                        if first:
                            first = False
                        else:
                            self.stdout.write('\n')
                        if cursor is not None:
                            pretty.pp_cursor(self.stdout, cursor)
            except (bayeslite.BayesDBException, bayeslite.BQLParseError) as e:
                self.stdout.write('%s\n' % (e,))
            except Exception:
                self.stdout.write(traceback.format_exc())
        else:
            self.prompt = self.bql_prompt
        return False

    def dot_help(self, line):
        '''show help for commands
        [<cmd> ...]

        Show help for commands.  With no arguments, list all commands.
        '''
        tokens = line.split()
        if len(tokens) == 0:
            pad = max(1 + len(cmd) for cmd in self._cmds)
            for cmnd in sorted(self._cmds):
                method = getattr(self, 'do_.%s' % (cmnd,))
                if method.__doc__ is not None:
                    doc = [l.strip() for l in method.__doc__.splitlines()]
                else:
                    doc = ['', '']
                assert 1 < len(doc)
                self.stdout.write(' %*s    %s\n' % (pad, '.' + cmnd, doc[0]))
            self.stdout.write('Type `.help <cmd>\''
                              ' for help on the command <cmd>.\n')
        else:
            for cmnd in tokens:
                if not hasattr(self, 'do_.%s' % (cmnd,)):
                    self.stdout.write('No such command %s.\n' % (repr(cmnd),))
                    return
                method = getattr(self, 'do_.%s' % (cmnd,))
                if method.__doc__ is not None:
                    doc = [l.strip() for l in method.__doc__.splitlines()]
                else:
                    self.stdout.write('No help for %s.\n' % (repr(cmnd),))
                    continue
                assert 1 < len(doc)
                self.stdout.write('.%s %s' % (cmnd, '\n'.join(doc[1:])))
                self.stdout.write('(END)\n\n')

    def dot_read(self, argsin):
        '''read a file of shell commands
        <path/to/file> [options]

        Adds a set of dot commands from the file at path to the cmdqueue.
        Options:
        -s : Sequential. Wait for keypress after each command has completed
        -c : Do not add comments to the command queue.
        -v : Verbose
        '''
        args = argsin.split()
        if len(args) == 0:
            self.stdout.write('Usage: .read <path/to/file> [options]\n')
            return
        path = args[0]
        sequential = False
        hide_comments = False
        verbose = False
        if len(args) > 1:
            for arg in set(args[1:]):
                if arg not in ['-s', '-c', '-v']:
                    self.stdout.write('Invalid argument %s\n' % (arg,))
                    return
                if arg == '-s':
                    sequential = True
                    verbose = True

                if arg == '-c':
                    hide_comments = True

                if arg == '-v':
                    verbose = True

        is_comment = lambda s: s.strip()[:2] == '--'
        is_continuation = lambda s: s.startswith(('\t', '  ', '    ',))

        try:
            f = open(path, 'rU')
        except Exception as e:
            self.stdout.write('%s\n' % (e,))
            return

        try:
            with f:
                padding = ' '*11
                cmds_exec = []
                cmds_disp = []
                cmd_exec = []
                cmd_disp = []
                for i, line in enumerate(f):
                    if (hide_comments and is_comment(line)) or line.isspace():
                        continue

                    if len(cmd_exec) == 0:
                        cmd_exec.append(line.strip())
                        cmd_disp.append(line)
                    elif is_continuation(line):
                        cmd_exec.append(line.strip())
                        cmd_disp.append(padding + line)
                    else:
                        cmds_exec.append(' '.join(cmd_exec) + '\n')
                        cmd_exec = [line.strip()]
                        cmds_disp.append(''.join(cmd_disp))
                        cmd_disp = [line]
                cmds_exec.append(' '.join(cmd_exec) + '\n')
                cmds_disp.append('\n'.join(cmd_disp))

                for cmd_disp, cmd_exec in zip(cmds_disp, cmds_exec):
                    if verbose:
                        self.stdout.write('bayeslite> ' + cmd_disp)
                    self.onecmd(cmd_exec)
                    if sequential:
                        raw_input('Press any key to continue.')
        except Exception as e:
            self.stdout.write('%s\n' % (e,))

    def _hook(self, cmdname, func, autorehook=False, yes=False, silent=False):
        import types
        if yes:
            autorehook = True

        if silent:
            do_print = lambda *s: None
        else:
            do_print = self.stdout.write

        if cmdname in self._cmds:
            affirmative = ['yes', 'y']
            negative = ['no', 'n']
            if not autorehook:
                self.stdout.write("Do you want to rehook the %s command?\n"
                    % (cmdname,))
                yesno = raw_input('y/n? ').lower()
                while yesno not in affirmative+negative:
                    self.stdout.write("Invalid response to yes/no question.\n")
                    yesno = raw_input('y/n? ').lower()
            else:
                yesno = 'yes'

            if yesno in affirmative:
                try:
                    self._uninstallcmd(cmdname)
                except ValueError as err:
                    self.stdout.write('%s\n' % (err,))
                    return
            else:
                do_print('skipping "%s".\n' % cmdname)
                return

        self._installcmd(cmdname, types.MethodType(func, self, type(self)))
        do_print('added command ".%s"\n' % (cmdname,))

    def dot_hook(self, path):
        '''add custom commands from a python source file
        <path_to_source.py>
        '''
        import imp

        if path in self._hooked_filenames:
            self.stdout.write("The file %s has already been hooked. Do you "
                              "want to rehook?\n" % (path,))
            yesno = raw_input('y/n? ')
            affirmative = ['yes', 'y']
            negative = ['no', 'n']
            while yesno.lower() not in affirmative+negative:
                self.stdout.write("Invalid response to yes/no question.\n")
                yesno = raw_input('y/n? ')

            if yesno in negative:
                self.stdout.write("Abandoning hook of %s\n" % (path,))
                return

        self.stdout.write('Loading hooks at %s...\n' % (path,))
        try:
            imp.load_source('bayeslite_shell_hooks', path)
        except Exception as e:
            self.stdout.write('%s\n' % (e,))
            self.stdout.write('Failed to load hooks: %s\n' % (path,))
        else:
            self._hooked_filenames.add(path)

    def dot_sql(self, line):
        '''execute a SQL query
        <query>

        Execute a SQL query on the underlying SQLite database.
        '''
        try:
            pretty.pp_cursor(self.stdout, self._bdb.sql_execute(line))
        except apsw.Error as e:
            self.stdout.write('%s\n' % (e,))
        except Exception as e:
            self.stdout.write(traceback.format_exc())
        return False

    def dot_open(self, line):
        '''close existing database and open new one
        <pathname>|-m

        Open the given filename, or a fresh memory-only database with -m.

        WARNING: Discards all existing state.
        '''
        # Need a completely new bdb object. Hope no one aliased it.
        if line == '-m':
            line = None
        self._bdb = bayeslite.bayesdb_open(pathname=line,
            builtin_metamodels=False)

    def dot_pythexec(self, line):
        '''execute a Python statement
        <statement>

        Execute a Python statement in the underlying Python
        interpreter.

        `bdb' is bound to the BayesDB instance.
        '''
        try:
            self._python_globals['bdb'] = self._bdb
            exec line in self._python_globals
        except Exception:
            self.stdout.write(traceback.format_exc())
        return False

    def dot_python(self, line):
        '''evaluate a Python expression
        <expression>

        Evaluate a Python expression in the underlying Python
        interpreter.

        `bdb' is bound to the BayesDB instance.

        Note that this command cannot execute a Python statement at
        the moment.  For that, use the `.pythexec` command.
        '''
        try:
            self._python_globals['bdb'] = self._bdb
            value = eval(line, self._python_globals)
            self.stdout.write('%s\n' % (repr(value),))
        except Exception:
            self.stdout.write(traceback.format_exc())
        return False

    def _trace(self, q, b):
        self.stdout.write('--> %s %s\n' % (q.strip(), b))

    def _sql_trace(self, q, b):
        self.stdout.write('==> %s %s\n' % (q.strip(), b))

    def dot_trace(self, line):
        '''trace queries
        [bql|sql]

        Trace BQL or SQL queries executed in the database.
        Use `.untrace' to undo.
        '''
        if line == 'bql':
            if not self._traced:
                self._bdb.trace(self._trace)
                self._traced = True
        elif line == 'sql':
            if not self._sql_traced:
                self._bdb.sql_trace(self._sql_trace)
                self._sql_traced = True
        else:
            self.stdout.write('Usage: .trace bql\n')
            self.stdout.write('       .trace sql\n')

    def dot_untrace(self, line):
        '''untrace queries
        [bql|sql]

        Untrace BQL or SQL queries executed in the database after
        `.trace' traced them.
        '''
        if line == 'bql':
            if self._traced:
                self._bdb.untrace(self._trace)
                self._traced = False
        elif line == 'sql':
            if self._sql_traced:
                self._bdb.sql_untrace(self._sql_trace)
                self._sql_traced = False
        else:
            self.stdout.write('Usage: .untrace bql\n')
            self.stdout.write('       .untrace sql\n')

    def dot_csv(self, line):
        '''create table from CSV file
        <table> </path/to/data.csv>

        Create a SQL table named <table> from the data in
        </path/to/data.csv>.
        '''
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 2:
            self.stdout.write('Usage: .csv <table> </path/to/data.csv>\n')
            return
        table = tokens[0]
        pathname = tokens[1]
        try:
            with open(pathname, 'rU') as f:
                bayeslite.bayesdb_read_csv(self._bdb, table, f, header=True,
                                           create=True, ifnotexists=False)
        except IOError as e:
            self.stdout.write('%s\n' % (e,))
        except Exception:
            self.stdout.write(traceback.format_exc())

    def dot_codebook(self, line):
        '''load codebook for table
        <table> </path/to/codebook.csv>

        Load a codebook -- short names, descriptions, and value
        descriptions for the columns of a table -- from a CSV file.
        '''
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 2:
            self.stdout.write('Usage: .codebook <table> '
                              '</path/to/codebook.csv>\n')
            return
        table = tokens[0]
        pathname = tokens[1]
        try:
            bayeslite.bayesdb_load_codebook_csv_file(self._bdb, table,
                                                     pathname)
        except IOError as e:
            self.stdout.write('%s\n' % (e,))
        except Exception:
            self.stdout.write(traceback.format_exc())

    def dot_guess(self, line):
        '''guess data generator
        <generator> <table>

        Create a generator named <generator> for the table <table>,
        guessing the statistical types of the columns in <table>.
        '''
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 2:
            self.stdout.write('Usage: .guess <generator> <table>\n')
            return
        generator = tokens[0]
        table = tokens[1]
        try:
            guess.bayesdb_guess_generator(self._bdb, generator, table,
                                          self._metamodel)
        except Exception:
            self.stdout.write(traceback.format_exc())

    def dot_legacymodels(self, line):
        '''load legacy models
        <generator> <table> </path/to/models.pkl.gz>

        Create a Crosscat generator named <generator> for the table
        <table> from the legacy models stored in
        </path/to/models.pkl.gz>.
        '''
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) != 3:
            self.stdout.write('Usage:'
                              ' .legacymodels <generator> <table>'
                              ' </path/to/models.pkl.gz>\n')
            return
        generator = tokens[0]
        table = tokens[1]
        pathname = tokens[2]
        try:
            bayeslite.bayesdb_load_legacy_models(self._bdb, generator, table,
                                                 self._metamodel, pathname,
                                                 create=True)
        except IOError as e:
            self.stdout.write('%s\n' % (e,))
        except Exception:
            self.stdout.write(traceback.format_exc())

    def dot_describe(self, line):
        '''describe BayesDB entities
        [table(s)|generator(s)|columns|model(s)] [<name>...]

        Print a human-readable description of the specified BayesDB
        entities.
        '''
        # XXX Lousy, lousy tokenizer.
        tokens = line.split()
        if len(tokens) == 0:
            self.stdout.write('Usage: .describe table(s) [<table>...]\n')
            self.stdout.write('       .describe generator(s) [<gen>...]\n')
            self.stdout.write('       .describe columns <gen>\n')
            self.stdout.write('       .describe model(s) <gen> [<model>...]\n')
            return
        if casefold(tokens[0]) == 'table' or \
           casefold(tokens[0]) == 'tables':
            params = None
            qualifier = None
            if len(tokens) == 1:
                params = ()
                qualifier = '1'
            else:
                params = tokens[1:]
                qualifier = \
                    '(' + ' OR '.join(['tabname = ?' for _p in params]) + ')'
                ok = True
                for table in params:
                    if not core.bayesdb_has_table(self._bdb, table):
                        self.stdout.write('No such table: %s\n' %
                                          (repr(table),))
                        ok = False
                if not ok:
                    return
                for table in params:
                    core.bayesdb_table_guarantee_columns(self._bdb, table)
            sql = '''
                SELECT tabname, colno, name, shortname
                    FROM bayesdb_column
                    WHERE %s
                    ORDER BY tabname ASC, colno ASC
            ''' % (qualifier,)
            with self._bdb.savepoint():
                pretty.pp_cursor(self.stdout, self._bdb.execute(sql, params))
        elif casefold(tokens[0]) == 'generator' or \
                casefold(tokens[0]) == 'generators':
            params = None
            qualifier = None
            if len(tokens) == 1:
                params = ()
                qualifier = '1'
            else:
                params = tokens[1:]
                names = ','.join('?%d' % (i + 1,) for i in range(len(params)))
                qualifier = '''
                    (name IN ({names}) OR (defaultp AND tabname IN ({names})))
                '''.format(names=names)
                ok = True
                for generator in params:
                    if not core.bayesdb_has_generator_default(self._bdb,
                            generator):
                        self.stdout.write('No such generator: %s\n' %
                            (repr(generator),))
                        ok = False
                if not ok:
                    return
            sql = '''
                SELECT id, name, tabname, metamodel
                    FROM bayesdb_generator
                    WHERE %s
            ''' % (qualifier,)
            with self._bdb.savepoint():
                pretty.pp_cursor(self.stdout,
                    self._bdb.sql_execute(sql, params))
        elif casefold(tokens[0]) == 'columns':
            if len(tokens) != 2:
                self.stdout.write('Describe columns of what generator?\n')
                return
            generator = tokens[1]
            with self._bdb.savepoint():
                if not core.bayesdb_has_generator_default(self._bdb,
                        generator):
                    self.stdout.write('No such generator: %s\n' %
                        (repr(generator),))
                    return
                generator_id = core.bayesdb_get_generator_default(self._bdb,
                    generator)
                sql = '''
                    SELECT c.colno AS colno, c.name AS name,
                            gc.stattype AS stattype, c.shortname AS shortname
                        FROM bayesdb_generator AS g,
                            (bayesdb_column AS c LEFT OUTER JOIN
                                bayesdb_generator_column AS gc
                                USING (colno))
                        WHERE g.id = ? AND g.id = gc.generator_id
                            AND g.tabname = c.tabname
                        ORDER BY colno ASC;
                '''
                cursor = self._bdb.sql_execute(sql, (generator_id,))
                pretty.pp_cursor(self.stdout, cursor)
        elif casefold(tokens[0]) == 'model' or \
                casefold(tokens[0]) == 'models':
            if len(tokens) < 2:
                self.stdout.write('Describe models of what generator?\n')
                return
            generator = tokens[1]
            with self._bdb.savepoint():
                if not core.bayesdb_has_generator_default(self._bdb,
                        generator):
                    self.stdout.write('No such generator: %s\n' %
                        (repr(generator),))
                    return
                generator_id = core.bayesdb_get_generator_default(self._bdb,
                    generator)
                qualifier = None
                if len(tokens) == 2:
                    qualifier = '1'
                else:
                    modelnos = []
                    for token in tokens[2:]:
                        try:
                            modelno = int(token)
                        except ValueError:
                            self.stdout.write('Invalid model number: %s\n' %
                                (repr(token),))
                            return
                        else:
                            if not core.bayesdb_generator_has_model(
                                    self._bdb, generator_id, modelno):
                                self.stdout.write('No such model: %d\n' %
                                    (modelno,))
                                return
                            modelnos.append(modelno)
                    qualifier = 'modelno IN (%s)' % \
                        (','.join(map(str, modelnos),))
                sql = '''
                    SELECT modelno, iterations FROM bayesdb_generator_model
                        WHERE generator_id = ? AND %s
                ''' % (qualifier,)
                cursor = self._bdb.sql_execute(sql, (generator_id,))
                pretty.pp_cursor(self.stdout, cursor)
        else:
            self.stdout.write('Usage: .describe table(s) [<table>...]\n')
            self.stdout.write('       .describe generator(s) [<gen>...]\n')
            self.stdout.write('       .describe columns <gen>\n')
            self.stdout.write('       .describe model(s) <gen> [<model>...]\n')
