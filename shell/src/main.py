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

import argparse
import os

import bayeslite
from bayeslite.metamodels.crosscat import CrosscatMetamodel
import bayeslite.shell.core as shell
import bayeslite.shell.hook as hook


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('bdbpath', type=str, nargs='?', default=None,
                        help="bayesdb database file")
    parser.add_argument('-j', '--jobs', type=int, default=1,
                        help="Max number of jobs (processes) useable.")
    parser.add_argument('-s', '--seed', type=int, default=None,
                        help="Random seed for the default generator.")
    parser.add_argument('-f', '--file', type=str, nargs=1, default=None,
                        help="Path to commands file. May be used to specify a "
                        "project-specific init file.")
    parser.add_argument('-b', '--batch', action='store_true',
                        help="Exit after executing file specified with -f.")
    parser.add_argument('-q', '--no-init-file', action='store_true',
                        help="Do not load ~/.bayesliterc")
    parser.add_argument('-m', '--memory', action='store_true',
                        help="Use temporary database not saved to disk")

    args = parser.parse_args(argv)
    return args


def run(stdin, stdout, stderr, argv):
    args = parse_args(argv[1:])
    progname = argv[0]
    slash = progname.rfind('/')
    if slash:
        progname = progname[slash + 1:]
    if args.bdbpath is None and not args.memory:
        stderr.write('%s: pass filename or -m/--memory\n' % (progname,))
        return 1
    if args.bdbpath == '-':
        stderr.write('%s: missing option?\n' % (progname,))
        return 1
    bdb = bayeslite.bayesdb_open(pathname=args.bdbpath,
        builtin_metamodels=False)

    if args.jobs != 1:
        import crosscat.MultiprocessingEngine as ccme
        jobs = args.jobs if args.jobs > 0 else None
        crosscat = ccme.MultiprocessingEngine(seed=args.seed, cpu_count=jobs)
    else:
        import crosscat.LocalEngine as ccle
        crosscat = ccle.LocalEngine(seed=args.seed)
    metamodel = CrosscatMetamodel(crosscat)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    bdbshell = shell.Shell(bdb, 'crosscat', stdin, stdout, stderr)
    with hook.set_current_shell(bdbshell):
        if not args.no_init_file:
            init_file = os.path.join(os.path.expanduser('~/.bayesliterc'))
            if os.path.isfile(init_file):
                bdbshell.dot_read(init_file)

        if args.file is not None:
            for path in args.file:
                if os.path.isfile(path):
                    bdbshell.dot_read(path)
                else:
                    bdbshell.stdout.write('%s is not a file.  Aborting.\n' %
                        (str(path),))
                    break

        if not args.batch:
            bdbshell.cmdloop()
    return 0


def main():
    import sys
    sys.exit(run(sys.stdin, sys.stdout, sys.stderr, sys.argv))

if __name__ == '__main__':
    main()
