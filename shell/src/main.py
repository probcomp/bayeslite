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

import getopt

import bayeslite
import bayeslite.shell.core as shell

def usage(stderr, argv):
    progname = argv[0]
    if progname.rfind('/'):
        progname = progname[progname.rfind('/') + 1:]
    stderr.write('Usage: %s [-j <njob>] [-s <seed>] [<file.bdb>]\n'
        % (progname,))
    return 1

def run(stdin, stdout, stderr, argv):
    njob = None
    seed = None
    try:
        opts, args = getopt.getopt(argv[1:], '?hj:s:', [])
    except getopt.GetoptError as e:
        stderr.write('%s\n' % (str(e),))
        return usage(stderr, argv)
    for o, a in opts:
        if o in ('-h', '-?'):
            return usage(stderr, argv)
        elif o == '-j':
            try:
                njob = int(a)
                if njob < 0:
                    raise ValueError
            except ValueError:
                stderr.write('%s: bad number of jobs\n' % (argv[0],))
                return 1
        elif o == '-s':
            try:
                seed = int(a)
            except ValueError:
                stderr.write('%s: bad seed\n' % (argv[0],))
                return 1
        else:
            assert False, 'bad option %s' % (o,)
    pathname = None
    if len(args) == 0:
        pathname = ':memory:'
    elif len(args) == 1:
        pathname = args[0]
    else:
        return usage(stderr, argv)
    # if seed is None:
    #     import os
    #     seedbuf = os.urandom(32)
    #     seed = 0
    #     for b in seedbuf:
    #         seed <<= 8
    #         seed |= ord(b)
    bdb = bayeslite.BayesDB(pathname=pathname)
    crosscat = None
    if njob:
        import crosscat.MultiprocessingEngine as ccme
        if njob == 0:
            njob = None
        crosscat = ccme.MultiprocessingEngine(seed=seed, cpu_count=njob)
    else:
        import crosscat.LocalEngine as ccle
        crosscat = ccle.LocalEngine(seed=seed)
    bayeslite.bayesdb_register_metamodel(bdb, 'crosscat', crosscat)
    bayeslite.bayesdb_set_default_metamodel(bdb, 'crosscat')
    shell.Shell(bdb).cmdloop()
    return 0

def main():
    import sys
    sys.exit(run(sys.stdin, sys.stdout, sys.stderr, sys.argv))

if __name__ == '__main__':
    main()
