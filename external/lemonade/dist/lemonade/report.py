'''
Procedures for generating reports and tables in the LEMON parser generator.
'''

from action import *
from acttab import *
from ccruft import *
from struct import *
from table import *

from sys import stderr


def file_makename(lemp, suffix):
    '''Generate a filename with the given suffix.'''

    from os.path import splitext

    name = splitext(lemp.filename)[0]
    name += suffix
    return name


def file_open(lemp, suffix, mode):
    '''Open a file with a name based on the name of the input file,
    but with a different (specified) suffix, and return the new
    stream.
    '''

    lemp.outname = file_makename(lemp, suffix)

    fp = None

    try:
        fp = open(lemp.outname, mode)
    except:
        if 'w' in mode:
            fprintf(stderr, "Can't open file \"%s\".\n", lemp.outname)
            lemp.errorcnt += 1

    return fp


def Reprint(lemp):
    '''Duplicate the input file without comments and without actions
    on rules.
    '''
    
    printf("// Reprint of input file \"%s\".\n// Symbols:\n", lemp.filename)

    maxlen = 10
    for i in range(lemp.nsymbol):
        sp = lemp.symbols[i]
        l = len(sp.name)
        if l > maxlen:
            maxlen = l

    ncolumns = 76 / (maxlen + 5)
    if ncolumns < 1:
        ncolumns = 1

    skip = (lemp.nsymbol + ncolumns - 1) / ncolumns
    for i in range(skip):
        printf("//")
        for j in range(i, lemp.nsymbol, skip):
            sp = lemp.symbols[j]
            assert sp.index == j
            printf(" %3d %-*.*s", j, maxlen, maxlen, sp.name)
        printf("\n")

    for rp in iterlinks(lemp.rule):
        printf("%s", rp.lhs.name)
        printf(" ::=")
        for i in range(rp.nrhs):
            sp = rp.rhs[i]
            printf(" %s", sp.name)
            if sp.type == MULTITERMINAL:
                for j in range(1, sp.nsubsym):
                    printf("|%s", sp.subsym[j].name)
        printf(".")
        if rp.precsym:
            printf(" [%s]", rp.precsym.name)
        printf("\n")

    return


def ConfigPrint(fp, cfp):
    rp = cfp.rp

    fprintf(fp, "%s ::=", rp.lhs.name)

    for i in range(rp.nrhs + 1):
        if i == cfp.dot:
            fprintf(fp, " *")

        if i == rp.nrhs:
            break

        sp = rp.rhs[i]
        fprintf(fp, " %s", sp.name)
        if sp.type == MULTITERMINAL:
            for j in range(1, sp.nsubsym):
                fprintf(fp, "|%s", sp.subsym[j].name)

    return


def PrintAction(ap, fp, indent, showPrecedenceConflict=False):
    '''Print an action to the given file stream.  Return False if
    nothing was actually printed.
    '''
    
    result = True

    if ap.type == SHIFT:
        fprintf(fp, "%*s shift  %d", indent, ap.sp.name, ap.x.stp.statenum)

    elif ap.type == REDUCE:
        fprintf(fp, "%*s reduce %d", indent, ap.sp.name, ap.x.rp.index)

    elif ap.type == ACCEPT:
        fprintf(fp, "%*s accept", indent, ap.sp.name)

    elif ap.type == ERROR:
        fprintf(fp, "%*s error", indent, ap.sp.name)

    elif ap.type in (SRCONFLICT, RRCONFLICT):
        fprintf(fp, "%*s reduce %-3d ** Parsing conflict **",
                indent, ap.sp.name, ap.x.rp.index)

    elif ap.type == SSCONFLICT:
        fprintf(fp, "%*s shift  %-3d ** Parsing conflict **",
                indent, ap.sp.name, ap.x.stp.statenum)

    elif ap.type == SH_RESOLVED:
        if showPrecedenceConflict:
            fprintf(fp, "%*s shift  %-3d -- dropped by precedence",
                    indent, ap.sp.name, ap.x.stp.statenum)
        else:
            result = False

    elif ap.type == RD_RESOLVED:
        if showPrecedenceConflict:
            fprintf(fp, "%*s reduce %-3d -- dropped by precedence",
                    indent, ap.sp.name, ap.x.rp.index)
        else:
            result = False

    elif ap.type == NOT_USED:
        result = False

    return result


def ReportOutput(lemp, showPrecedenceConflict=False):
    '''Generate the "y.output" log file.'''

    from set import SetFind

    fp = file_open(lemp, ".out", "wb")
    if fp is None:
        return

    for i in range(lemp.nstate):
        stp = lemp.sorted[i]
        fprintf(fp, "State %d:\n", stp.statenum)
        if lemp.basisflag:
            cfp = stp.bp
        else:
            cfp = stp.cfp

        while cfp:
            if cfp.dot == cfp.rp.nrhs:
                buf = "(%d)" % cfp.rp.index
                fprintf(fp, "    %5s ", buf)
            else:
                fprintf(fp, "          ")

            ConfigPrint(fp, cfp)
            fprintf(fp, "\n")
            if lemp.basisflag:
                cfp = cfp.bp
            else:
                cfp = cfp.next

        fprintf(fp, "\n")
        for ap in iterlinks(stp.ap):
            if PrintAction(ap, fp, 30, showPrecedenceConflict):
                fprintf(fp, "\n")

        fprintf(fp, "\n")

    fprintf(fp, "----------------------------------------------------\n")
    fprintf(fp, "Symbols:\n")

    for i in range(lemp.nsymbol):
        sp = lemp.symbols[i]
        fprintf(fp, "  %3d: %s", i, sp.name)
        if sp.type == NONTERMINAL:
            fprintf(fp, ":")
            if sp._lambda:
                fprintf(fp, " <lambda>")

            for j in range(lemp.nterminal):
                if sp.firstset and SetFind(sp.firstset, j):
                    fprintf(fp, " %s", lemp.symbols[j].name)

        fprintf(fp, "\n")

    fp.close()

    return


def compute_action(lemp, ap):
    '''Given an action, compute the integer value for that action
    which is to be put in the action table of the generated machine.
    Return negative if no action should be generated.
    '''
    
    if ap.type == SHIFT:
        act = ap.x.stp.statenum

    elif ap.type == REDUCE:
        act = ap.x.rp.index + lemp.nstate

    elif ap.type == ERROR:
        act = lemp.nstate + lemp.nrule

    elif ap.type == ACCEPT:
        act = lemp.nstate + lemp.nrule + 1

    else:
        act = -1

    return act



# The next cluster of routines are for reading the template file and
# writing the results to the generated parser.


def tplt_xfer(name, _in, out):
    '''Transfer data from "in" to "out" until a line is seen which
    contains "%%".  Return the text leading up to the "%%".

    If "name" is given, then any class declaration for "Parser" is
    changed to be the given name instead.
    '''

    import re

    indent = ''

    for line in _in:
        pos = line.find('%%')
        if pos != -1:
            indent = line[:pos]
            break

        if name:
            l = re.split('(class )(Parser)', line)
            line = ''.join([name if s == 'Parser' else s for s in l])

        fprintf(out, "%s", line)

    return indent


def tplt_open(lemp):
    '''Find the template file and open it, returning a stream.'''

    from os.path import dirname, splitext, isfile
    import os
    
    templatename = "lempar.tmpl"
    buf = splitext(lemp.filename)[0] + ".lt"

    if isfile(buf):
        tpltname = buf
    elif isfile(templatename):
        tpltname = templatename
    else:
        from os.path import dirname, join
        tpltname = join(dirname(__file__), templatename)
        if not isfile(tpltname):
            tpltname = None

    if tpltname is None and os.sep in lemp.filename:
        # 2009-07-16 lcs
        buf = join(dirname(lemp.filename), templatename)
        if isfile(buf):
            tpltname = buf

    if tpltname is None:
        fprintf(stderr,
                "Can't find the parser driver template file \"%s\".\n",
                templatename)
        lemp.errorcnt += 1
        return None

    try:
        _in = open(tpltname, "rb")
    except IOError:
        fprintf(stderr,
                "Can't open the template file \"%s\".\n",
                templatename)
        lemp.errorcnt += 1
        return None

    return _in


def tplt_print(out, lemp, str):
    '''Print a string to the file.'''

    if not str:
        return

    out.write(str)

    if str[-1] != '\n':
        fputc('\n', out)

    return


def generate_action(out, indent, lemp, rp):
    fprintf(out, "%sdef action_%03d(self):\n", indent, rp.index)
    fprintf(out, "%s    # ", indent)
    writeRuleText(out, rp)
    fprintf(out, "\n")
    if rp.lhsalias:
        name = "p_%s_%s" % (rp.lhs.name, rp.lhsalias)
        fprintf(out, "%s    return self.delegate.%s(\n", indent, name)
        for i in range(rp.nrhs):
            if rp.rhsalias[i]:
                fprintf(out,
                        "%s        %s = self.yystack[%d].minor,\n",
                        indent, rp.rhsalias[i], i - rp.nrhs)
        fprintf(out, "%s        )\n", indent)
    else:
        fprintf(out, "%s    return None\n", indent)
    return


# Each state contains a set of token transaction and a set of
# nonterminal transactions.  Each of these sets makes an instance of
# the following structure.  An array of these structures is used to
# order the creation of entries in the yy_action[] table.

axset = struct(
    'axset', (
        'stp',      # A state
        'isTkn',    # True to use tokens.  False for non-terminals
        'nAction',  # Number of actions
        'iOrder',   # Original order of action sets
        )
    )


def axset_compare(a, b):
    '''Compare to axset structures for sorting purposes.'''
    c = b.nAction - a.nAction
    if c == 0:
        c = b.iOrder - a.iOrder
    assert c != 0 or a == b
    return c


def writeRuleText(out, rp):
    '''Write text on "out" that describes the rule "rp".'''
    fprintf(out, "%s ::=", rp.lhs.name)
    for j in range(rp.nrhs):
        sp = rp.rhs[j]
        fprintf(out, " %s", sp.name)
        if sp.type == MULTITERMINAL:
            for k in range(1, sp.nsubsym):
                fprintf(out, "|%s", sp.subsym[k].name)
    return


def ReportTable(lemp, outputStream=None):
    '''Generate C source code for the parser.'''

    _in = tplt_open(lemp)
    if _in is None:
        return

    if outputStream is None:
        out = file_open(lemp, ".py", "wb")
        if out is None:
            _in.close()
            return
    else:
        out = outputStream


    indent = tplt_xfer(lemp.name, _in, out)


    #
    # Generate definitions for all tokens
    #
    
    prefix = lemp.tokenprefix or ""
    for i in range(1, lemp.nterminal):
        fprintf(out, "%s%s%-30s = %2d\n",
                indent, prefix, lemp.symbols[i].name, i)

    indent = tplt_xfer(lemp.name, _in, out)


    #
    # Generate the defines
    #
    
    fprintf(out, "%sYYNOCODE = %d\n", indent, lemp.nsymbol + 1)
    if lemp.wildcard:
        fprintf(out, "%sYYWILDCARD = %d\n",
                indent, lemp.wildcard.index)

    fprintf(out, "%sYYNSTATE = %d\n", indent, lemp.nstate)
    fprintf(out, "%sYYNRULE = %d\n", indent, lemp.nrule)
    if lemp.errsym.useCnt:
        fprintf(out, "%sYYERRORSYMBOL = %d\n", indent, lemp.errsym.index)

    indent = tplt_xfer(lemp.name, _in, out)


    # Generate the action table and its associates:
    #
    #  yy_action[]        A single table containing all actions.
    #  yy_lookahead[]     A table containing the lookahead for each entry in
    #                     yy_action.  Used to detect hash collisions.
    #  yy_shift_ofst[]    For each state, the offset into yy_action for
    #                     shifting terminals.
    #  yy_reduce_ofst[]   For each state, the offset into yy_action for
    #                     shifting non-terminals after a reduce.
    #  yy_default[]       Default action for each state.


    #
    # Compute the actions on all states and count them up
    #

    ax = [None] * (lemp.nstate * 2)

    for i in range(lemp.nstate):
        stp = lemp.sorted[i]
        ax[i*2] = axset(
            stp = stp,
            isTkn = True,
            nAction = stp.nTknAct,
            iOrder = -1,
            )
        ax[i*2+1] = axset(
            stp = stp,
            isTkn = False,
            nAction = stp.nNtAct,
            iOrder = -1,
            )


    # Compute the action table.  In order to try to keep the size of
    # the action table to a minimum, the heuristic of placing the
    # largest action sets first is used.

    mxTknOfst = mnTknOfst = 0
    mxNtOfst = mnNtOfst = 0

    for i in range(lemp.nstate*2):
        ax[i].iOrder = i
    ax.sort(cmp = axset_compare)
    pActtab = acttab_alloc()

    i = 0
    while i < lemp.nstate*2 and ax[i].nAction > 0:
        stp = ax[i].stp

        if ax[i].isTkn:
            for ap in iterlinks(stp.ap):
                if ap.sp.index >= lemp.nterminal:
                    continue

                action = compute_action(lemp, ap)
                if action < 0:
                    continue

                acttab_action(pActtab, ap.sp.index, action)

            stp.iTknOfst = acttab_insert(pActtab)
            if stp.iTknOfst < mnTknOfst:
                mnTknOfst = stp.iTknOfst

            if stp.iTknOfst > mxTknOfst:
                mxTknOfst = stp.iTknOfst

        else:
            for ap in iterlinks(stp.ap):
                if ap.sp.index < lemp.nterminal:
                    continue

                if ap.sp.index == lemp.nsymbol:
                    continue

                action = compute_action(lemp, ap)
                if action < 0:
                    continue

                acttab_action(pActtab, ap.sp.index, action)

            stp.iNtOfst = acttab_insert(pActtab)

            if stp.iNtOfst < mnNtOfst:
                mnNtOfst = stp.iNtOfst
            if stp.iNtOfst > mxNtOfst:
                mxNtOfst = stp.iNtOfst

        i += 1
    
    ax = None


    #
    # Output the yy_action table
    #

    n = acttab_size(pActtab)
    fprintf(out, "%sYY_ACTTAB_COUNT = %d\n", indent, n)
    fprintf(out, "%syy_action = [\n", indent)

    j = 0
    for i in range(n):
        action = acttab_yyaction(pActtab, i)
        if action < 0:
            action = lemp.nstate + lemp.nrule + 2

        if j == 0:
            fprintf(out, "%s    ", indent)

        fprintf(out, " %4d,", action)

        if j == 9 or i == n - 1:
            fprintf(out, " # %5d\n", i - j)
            j = 0
        else:
            j += 1

    fprintf(out, "%s    ]\n", indent)

    #
    # Output the yy_lookahead table
    #
    
    fprintf(out, "%syy_lookahead = [\n", indent)

    j = 0
    for i in range(n):
        la = acttab_yylookahead(pActtab, i)
        if la < 0:
            la = lemp.nsymbol

        if j == 0:
            fprintf(out, "%s    ", indent)

        fprintf(out, " %4d,", la)
        if j == 9 or i == n - 1:
            fprintf(out, " # %5d\n", i - j)
            j = 0
        else:
            j += 1

    fprintf(out, "%s    ]\n", indent)


    #
    # Output the yy_shift_ofst[] table
    #

    fprintf(out, "%sYY_SHIFT_USE_DFLT = %d\n", indent, mnTknOfst - 1)

    n = lemp.nstate
    while n > 0 and lemp.sorted[n-1].iTknOfst == NO_OFFSET:
        n -= 1

    fprintf(out, "%sYY_SHIFT_COUNT = %d\n", indent, n - 1)
    fprintf(out, "%sYY_SHIFT_MIN = %d\n", indent, mnTknOfst)
    fprintf(out, "%sYY_SHIFT_MAX = %d\n", indent, mxTknOfst)
    fprintf(out, "%syy_shift_ofst = [\n", indent)

    j = 0
    for i in range(n):
        stp = lemp.sorted[i]
        ofst = stp.iTknOfst

        if ofst == NO_OFFSET:
            ofst = mnTknOfst - 1

        if j == 0:
            fprintf(out, "%s    ", indent)

        fprintf(out, " %4d,", ofst)

        if j == 9 or i == n - 1:
            fprintf(out, " # %5d\n", i - j)
            j = 0
        else:
            j += 1

    fprintf(out, "%s    ]\n", indent)


    #
    # Output the yy_reduce_ofst[] table
    #
    
    fprintf(out, "%sYY_REDUCE_USE_DFLT = %d\n", indent, mnNtOfst - 1)
    
    n = lemp.nstate
    while n > 0 and lemp.sorted[n-1].iNtOfst == NO_OFFSET:
        n -= 1

    fprintf(out, "%sYY_REDUCE_COUNT = %d\n", indent, n - 1)
    fprintf(out, "%sYY_REDUCE_MIN = %d\n", indent, mnNtOfst)
    fprintf(out, "%sYY_REDUCE_MAX = %d\n", indent, mxNtOfst)
    fprintf(out, "%syy_reduce_ofst = [\n", indent)

    j = 0
    for i in range(n):
        stp = lemp.sorted[i]
        ofst = stp.iNtOfst

        if ofst == NO_OFFSET:
            ofst = mnNtOfst - 1

        if j == 0:
            fprintf(out, "%s    ", indent)

        fprintf(out, " %4d,", ofst)

        if j == 9 or i == n - 1:
            fprintf(out, " # %5d\n", i - j)
            j = 0
        else:
            j += 1

    fprintf(out, "%s    ]\n", indent)


    #
    # Output the default action table
    #
    
    fprintf(out, "%syy_default = [\n", indent)

    n = lemp.nstate

    j = 0
    for i in range(n):
        stp = lemp.sorted[i]

        if j == 0:
            fprintf(out, "%s    ", indent)

        fprintf(out, " %4d,", stp.iDflt)

        if j == 9 or i == n - 1:
            fprintf(out, " # %5d\n", i - j)
            j = 0
        else:
            j += 1

    fprintf(out, "%s    ]\n", indent)

    indent = tplt_xfer(lemp.name, _in, out)


    #
    # Generate the table of fallback tokens.
    #

    if lemp.has_fallback:
        mx = lemp.nterminal - 1
        while mx > 0 and lemp.symbols[mx].fallback is None:
            mx -= 1
        for i in range(mx + 1):
            p = lemp.symbols[i]
            if p.fallback is None:
                fprintf(out, "%s  0,  # %10s => nothing\n", indent, p.name)
            else:
                fprintf(out, "%s%3d,  # %10s => %s\n",
                        indent, p.fallback.index, p.name, p.fallback.name)

    indent = tplt_xfer(lemp.name, _in, out)


    #
    # Generate a table containing the symbolic name of every symbol
    #

    for i in range(lemp.nsymbol):
        line = "\"%s\"," % lemp.symbols[i].name
        fprintf(out, "%s%-15s", indent, line)
        
        if (i & 3) == 3:
            fprintf(out, "\n")

    if (i & 3) != 3:
        fprintf(out, "\n")

    indent = tplt_xfer(lemp.name, _in, out)


    # Generate a table containing a text string that describes every
    # rule in the rule set of the grammer.  This information is used
    # when tracing REDUCE actions.

    for i, rp in enumerate(iterlinks(lemp.rule)):
        assert rp.index == i
        fprintf(out, "%s\"", indent)
        writeRuleText(out, rp)
        fprintf(out, "\", # %3d\n", i)

    indent = tplt_xfer(lemp.name, _in, out)

    
    # Generate the table of rule information 
    #
    # Note: This code depends on the fact that rules are number
    # sequentually beginning with 0.
  
    for rp in iterlinks(lemp.rule):
        fprintf(out, "%syyRuleInfoEntry( %d, %d ),\n", indent, rp.lhs.index, rp.nrhs)

    indent = tplt_xfer(lemp.name, _in, out)


    #
    # Generate code which execution during each REDUCE action
    #
    
    for rp in iterlinks(lemp.rule):
        generate_action(out, indent, lemp, rp)

    fprintf(out, "%syy_action_method = [\n", indent)
    for i, rp in enumerate(iterlinks(lemp.rule)):
        assert i == rp.index
        fprintf(out, "%s    action_%03d,\n", indent, i)
    fprintf(out, "%s]\n", indent)


    tplt_xfer(lemp.name, _in, out)


    _in.close()
    if not outputStream:
        out.close()

    return


def CompressTables(lemp):
    '''Reduce the size of the action tables, if possible, by making use
    of defaults.'''

    # In this version, we take the most frequent REDUCE action and
    # make it the default.  Except, there is no default if the
    # wildcard token is a possible look-ahead.

    for i in range(lemp.nstate):
        stp = lemp.sorted[i]
        nbest = 0
        rbest = None
        usesWildcard = False
        
        for ap in iterlinks(stp.ap):
            if ap.type == SHIFT and ap.sp == lemp.wildcard:
                usesWildcard = True

            if ap.type != REDUCE:
                continue

            rp = ap.x.rp
            if rp.lhsStart:
                continue

            if rp == rbest:
                continue

            n = 1
            for ap2 in iterlinks(ap.next):
                if ap2.type != REDUCE:
                    continue

                rp2 = ap2.x.rp
                if rp2 == rbest:
                    continue

                if rp2 == rp:
                    n += 1

            if n > nbest:
                nbest = n
                rbest = rp

        # Do not make a default if the number of rules to default is
        # not at least 1 or if the wildcard token is a possible
        # lookahead.
        if nbest < 1 or usesWildcard:
            continue

        # Combine matching REDUCE actions into a single default
        for ap in iterlinks(stp.ap):
            if ap.type == REDUCE and ap.x.rp == rbest:
                break
        assert ap
        ap.sp = Symbol_new("{default}")
        for ap in iterlinks(ap.next):
            if ap.type == REDUCE and ap.x.rp == rbest:
                ap.type = NOT_USED
        stp.ap = Action_sort(stp.ap)

    return


def stateResortCompare(a, b):
    '''Compare two states for sorting purposes.  The smaller state is
    the one with the most non-terminal actions.  If they have the same
    number of non-terminal actions, then the smaller is the one with
    the most token actions.
    '''
    n = b.nNtAct - a.nNtAct
    if n == 0:
        n = b.nTknAct - a.nTknAct
        if n == 0:
            n = b.statenum - a.statenum
    assert n != 0
    return n


def ResortStates(lemp):
    '''Renumber and resort states so that states with fewer choices
    occur at the end.  Except, keep state 0 as the first state.
    '''

    for i in range(lemp.nstate):
        stp = lemp.sorted[i]
        stp.nTknAct = stp.nNtAct = 0
        stp.iDflt = lemp.nstate + lemp.nrule
        stp.iTknOfst = NO_OFFSET
        stp.iNtOfst = NO_OFFSET

        for ap in iterlinks(stp.ap):
            if compute_action(lemp, ap) >= 0:
                if ap.sp.index < lemp.nterminal:
                    stp.nTknAct += 1
                elif ap.sp.index < lemp.nsymbol:
                    stp.nNtAct += 1
                else:
                    stp.iDflt = compute_action(lemp, ap)

    lemp.sorted = ([lemp.sorted[0]] +
                   sorted(lemp.sorted[1:], cmp = stateResortCompare))

    for i in range(lemp.nstate):
        lemp.sorted[i].statenum = i

    return

