'''
Routines to processing a configuration list and building a state in
the LEMON parser generator.
'''


from ccruft import iterlinks
from error import *
from set import *
from struct import *
from table import *


current = None      # Top of list of configurations
currentend = None   # Last on list of configs
basis = None        # Top of list of basis configs
basisend = None     # End of list of basis configs


def config_model(rp, dot):
    return config(
        rp = rp,
        dot = dot,
        fws = None,
        fplp = None,
        bplp = None,
        stp = None,
        status = 0,
        next = None,
        bp = None,
        )


def Configlist_init():
    '''Initialize the configuration list builder.'''
    global current, currentend
    global basis, basisend
    current = currentend = None
    basis = basisend = None
    Configtable_init()
    return


def Configlist_reset():
    '''Initialize the configuration list builder.'''
    global current, currentend
    global basis, basisend
    current = currentend = None
    basis = basisend = None
    Configtable_clear()
    return


def Configlist_add(rp, dot):
    '''Add another configuration to the configuration list.

    rp:   The rule
    dot:  Index into the RHS of the rule where the dot goes
    '''

    global current, currentend

    model = config_model(rp, dot)
    cfp = Configtable_find(model)
    if cfp is None:
        cfp = config(
            rp = rp,
            dot = dot,
            fws = SetNew(),
            fplp = None,
            bplp = None,
            stp = None,
            status = 0,
            next = None,
            bp = None,
            )

        if currentend:
            currentend.next = cfp
        else:
            current = cfp
        currentend = cfp

        Configtable_insert(cfp)
    return cfp


def Configlist_addbasis(rp, dot):
    '''Add a basis configuration to the configuration list.'''

    global current, currentend
    global basis, basisend

    model = config_model(rp, dot)
    cfp = Configtable_find(model)
    if cfp is None:
        cfp = config(
            rp = rp,
            dot = dot,
            fws = SetNew(),
            fplp = None,
            bplp = None,
            stp = None,
            status = 0,
            next = None,
            bp = None,
            )

        if currentend:
            currentend.next = cfp
        else:
            current = cfp
        currentend = cfp

        if basisend:
            basisend.bp = cfp
        else:
            basis = cfp
        basisend = cfp

        Configtable_insert(cfp)

    return cfp


def Configlist_closure(lemp):
    '''Compute the closure of the configuration list.'''

    from plink import Plink_add

    for cfp in iterlinks(current):
        rp = cfp.rp
        dot = cfp.dot
        if dot >= rp.nrhs:
            continue
        sp = rp.rhs[dot]
        if sp.type == NONTERMINAL:
            if sp.rule is None and sp != lemp.errsym:
                ErrorMsg(lemp.filename, rp.line,
                         'Nonterminal "%s" has no rules.',
                         sp.name)
                lemp.errorcnt += 1
            for newrp in iterlinks(sp.rule, 'nextlhs'):
                newcfp = Configlist_add(newrp, 0)
                for i in range(dot + 1, rp.nrhs):
                    xsp = rp.rhs[i]
                    if xsp.type == TERMINAL:
                        SetAdd(newcfp.fws, xsp.index)
                        break
                    elif xsp.type == MULTITERMINAL:
                        for k in range(xsp.nsubsym):
                            SetAdd(newcfp.fws, xsp.subsym[k].index)
                        break
                    else:
                        SetUnion(newcfp.fws, xsp.firstset)
                        if not xsp._lambda:
                            break
                else:
                    cfp.fplp = Plink_add(cfp.fplp, newcfp)
    return


def Configlist_sort():
    '''Sort the configuration list.'''
    from msort import msort
    global current, currentend
    current = msort(current, 'next', Configcmp)
    currentend = None
    return


def Configlist_sortbasis():
    '''Sort the basis configuration list.'''
    from msort import msort
    global basis, basisend
    basis = msort(current, 'bp', Configcmp)
    basisend = None
    return


def Configlist_return():
    '''Return a pointer to the head of the configuration list and
    reset the list.
    '''
    global current, currentend
    old = current
    current = None
    currentend = None
    return old


def Configlist_basis():
    '''Return a pointer to the head of the configuration list and
    reset the list.'''
    global basis, basisend
    old = basis
    basis = None
    basisend = None
    return old
