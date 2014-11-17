'''
Input file parser for the LEMON parser generator.
'''

from ccruft import *
from error import *
from struct import *
from table import *

from sys import exit


MAXRHS = 1000


(
    INITIALIZE,
    WAITING_FOR_DECL_OR_RULE,
    WAITING_FOR_DECL_KEYWORD,
    WAITING_FOR_DECL_ARG,
    WAITING_FOR_PRECEDENCE_SYMBOL,
    WAITING_FOR_ARROW,
    IN_RHS,
    LHS_ALIAS_1,
    LHS_ALIAS_2,
    LHS_ALIAS_3,
    RHS_ALIAS_1,
    RHS_ALIAS_2,
    PRECEDENCE_MARK_1,
    PRECEDENCE_MARK_2,
    RESYNC_AFTER_RULE_ERROR,
    RESYNC_AFTER_DECL_ERROR,
    WAITING_FOR_FALLBACK_ID,
    WAITING_FOR_WILDCARD_ID,
    ) = range(18)


pstate = struct(
    'pstate',
    (
        'filename',         # Name of the input file
        'tokenlineno',      # Linenumber at which current token starts
        'errorcnt',         # Number of errors so far
        'tokenstart',       # Text of current token
        'gp',               # Global state vector
        'state',            # The state of the parser
        'fallback',         # The fallback token
        'lhs',              # Left-hand side of current rule
        'lhsalias',         # Alias for the LHS
        'nrhs',             # Number of right-hand side symbols seen
        'rhs',              # RHS symbols
        'alias',            # Aliases for each RHS symbol (or NULL)
        'prevrule',         # Previous rule parsed
        'declkeyword',      # Keyword of a declaration
        'declargslot',      # Where the declaration argument should be put
        'insertLineMacro',  # Add #line before declaration insert
        'declassoc',        # Assign this association to decl arguments
        'preccounter',      # Assign this precedence to decl arguments
        'firstrule',        # Pointer to first rule in the grammar
        'lastrule',         # Pointer to the most recently parsed rule
        )
    )


def parseonetoken(psp, x):
    '''Parse a single token.'''

    x = Strsafe(x) # Save the token permanently
    
    if psp.state == INITIALIZE:
        psp.prevrule = None
        psp.preccounter = 0
        psp.firstrule = psp.lastrule = None
        psp.gp.nrule = 0
        psp.state = WAITING_FOR_DECL_OR_RULE


    if psp.state == WAITING_FOR_DECL_OR_RULE:
        if x[0] == '%':
            psp.state = WAITING_FOR_DECL_KEYWORD
        elif x[0].islower():
            psp.lhs = Symbol_new(x)
            psp.nrhs = 0
            psp.lhsalias = None
            psp.state = WAITING_FOR_ARROW
        elif x[0] == '{':
            if True:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         "Code fragment actions are not supported.")
                psp.errorcnt += 1
            elif psp.prevrule is None:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         "There is not prior rule upon which to attach the code "
                         "fragment which begins on this line.")
                psp.errorcnt += 1
            elif psp.prevrule.code is not None:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         "Code fragment beginning on this line is not the first "
                         "to follow the previous rule.")
                psp.errorcnt += 1
            else:
                psp.prevrule.line = psp.tokenlineno
                psp.prevrule.code = x[1:]

        elif x[0] == '[':
            psp.state = PRECEDENCE_MARK_1
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Token "%s" should be either "%%" or a nonterminal name.',
                     x)
            psp.errorcnt += 1

    elif psp.state == PRECEDENCE_MARK_1:
        if not x[0].isupper():
            ErrorMsg(psp.filename, psp.tokenlineno,
                     "The precedence symbol must be a terminal.")
            psp.errorcnt += 1
        elif psp.prevrule is None:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'There is no prior rule to assign precedence "[%s]".', x)
            psp.errorcnt += 1
        elif psp.prevrule.precsym is not None:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     "Precedence mark on this line is not the first to follow the previous rule.")
            psp.errorcnt += 1
        else:
            psp.prevrule.precsym = Symbol_new(x)

        psp.state = PRECEDENCE_MARK_2

    elif psp.state == PRECEDENCE_MARK_2:
        if x[0] != ']':
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Missing "]" on precedence mark.')
            psp.errorcnt += 1

        psp.state = WAITING_FOR_DECL_OR_RULE

    elif psp.state == WAITING_FOR_ARROW:
        if x[:3] == '::=':
            psp.state = IN_RHS
        elif x[0] == '(':
            psp.state = LHS_ALIAS_1
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Expected to see a ":" following the LHS symbol "%s".',
                     psp.lhs.name)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == LHS_ALIAS_1:
        if x[0].isalpha():
            psp.lhsalias = x
            psp.state = LHS_ALIAS_2
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     '"%s" is not a valid alias for the LHS "%s"\n',
                     x, psp.lhs.name)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == LHS_ALIAS_2:
        if x[0] == ')':
            psp.state = LHS_ALIAS_3
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Missing ")" following LHS alias name "%s".',
                     psp.lhsalias)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == LHS_ALIAS_3:
        if x[:3] == '::=':
            psp.state = IN_RHS
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Missing "->" following: "%s(%s)".',
                     psp.lhs.name, psp.lhsalias)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == IN_RHS:
        if x[0] == '.':
            rp = rule(
                ruleline = psp.tokenlineno,
                rhs = psp.rhs[:psp.nrhs],
                rhsalias = psp.alias[:psp.nrhs],
                lhs = psp.lhs,
                lhsalias = psp.lhsalias,
                nrhs = psp.nrhs,
                code = None,
                precsym = None,
                index = psp.gp.nrule,
                lhsStart = False,
                line = 0,
                canReduce = False,
                nextlhs = None,
                next = None,
                )
            psp.gp.nrule += 1
            rp.nextlhs = rp.lhs.rule
            rp.lhs.rule = rp
            if psp.firstrule is None:
                psp.firstrule = psp.lastrule = rp
            else:
                psp.lastrule.next = rp
                psp.lastrule = rp
            psp.prevrule = rp

            psp.state = WAITING_FOR_DECL_OR_RULE

        elif x[0].isalpha():
            if psp.nrhs >= MAXRHS:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         'Too many symbols on RHS of rule beginning at "%s".',
                         x)
                psp.errorcnt += 1
                psp.state = RESYNC_AFTER_RULE_ERROR
            else:
                psp.rhs[psp.nrhs] = Symbol_new(x)
                psp.alias[psp.nrhs] = None
                psp.nrhs += 1

        elif x[0] in ('|', '/') and psp.nrhs > 0:
            msp = psp.rhs[psp.nrhs - 1]
            if msp.type != MULTITERMINAL:
                origsp = msp
                msp = symbol(
                    type = MULTITERMINAL,
                    nsubsym = 1,
                    subsym = [origsp],
                    name = origsp.name,
                    index = 0,
                    rule = None,
                    fallback = None,
                    prec = 0,
                    assoc = 0,
                    firstset = None,
                    _lambda = False,
                    useCnt = 0,
                    )
                psp.rhs[psp.nrhs - 1] = msp

            msp.nsubsym += 1
            msp.subsym.append(Symbol_new(x[1:]))
            if x[1].islower() or msp.subsym[0].name[0].islower():
                ErrorMsg(psp.filename, psp.tokenlineno,
                         "Cannot form a compound containing a non-terminal")
                psp.errorcnt += 1

        elif x[0] == '(' and psp.nrhs > 0:
            psp.state = RHS_ALIAS_1
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Illegal character on RHS of rule: "%s".', x)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == RHS_ALIAS_1:
        if x[0].isalpha():
            psp.alias[psp.nrhs - 1] = x
            psp.state = RHS_ALIAS_2
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     '"%s" is not a valid alias for the RHS symbol "%s"\n',
                     x, psp.rhs[psp.nrhs - 1].name)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == RHS_ALIAS_2:
        if x[0] == ')':
            psp.state = IN_RHS
        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Missing ")" following LHS alias name "%s".',
                     psp.lhsalias)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_RULE_ERROR

    elif psp.state == WAITING_FOR_DECL_KEYWORD:
        if x[0].isalpha():
            psp.declkeyword = x
            psp.declargslot = None
            psp.insertLineMacro = True
            psp.state = WAITING_FOR_DECL_ARG
            if strcmp(x, "name") == 0:
                psp.declargslot = 'name'
                psp.insertLineMacro = False
            elif strcmp(x, "token_prefix") == 0:
                psp.declargslot = 'tokenprefix'
                psp.insertLineMacro = False
            elif strcmp(x, "start_symbol") == 0:
                psp.declargslot = 'start'
                psp.insertLineMacro = False
            elif strcmp(x, "left") == 0:
                psp.preccounter += 1
                psp.declassoc = LEFT
                psp.state = WAITING_FOR_PRECEDENCE_SYMBOL
            elif strcmp(x, "right") == 0:
                psp.preccounter += 1
                psp.declassoc = RIGHT
                psp.state = WAITING_FOR_PRECEDENCE_SYMBOL
            elif strcmp(x, "nonassoc") == 0:
                psp.preccounter += 1
                psp.declassoc = NONE
                psp.state = WAITING_FOR_PRECEDENCE_SYMBOL
            elif strcmp(x, "fallback") == 0:
                psp.fallback = None
                psp.state = WAITING_FOR_FALLBACK_ID
            elif strcmp(x, "wildcard") == 0:
                psp.state = WAITING_FOR_WILDCARD_ID
            else:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         'Unknown declaration keyword: "%%%s".', x)
                psp.errorcnt += 1
                psp.state = RESYNC_AFTER_DECL_ERROR

        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     'Illegal declaration keyword: "%s".', x)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_DECL_ERROR

    elif psp.state == WAITING_FOR_PRECEDENCE_SYMBOL:
        if x[0] == '.':
            psp.state = WAITING_FOR_DECL_OR_RULE
        elif x[0].isupper():
            sp = Symbol_new(x)
            if sp.prec >= 0:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         'Symbol "%s" has already be given a precedence.',
                         x)
                psp.errorcnt += 1
            else:
                sp.prec = psp.preccounter
                sp.assoc = psp.declassoc

        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     """Can't assign a precedence to "%s".""", x)
            psp.errorcnt += 1

    elif psp.state == WAITING_FOR_DECL_ARG:
        if x[0] in ('{', '"') or x[0].isalnum():
            zNew = x
            if zNew[0] in ('"', '{'):
                zNew = zNew[1:]

            zOld = getattr(psp.gp, psp.declargslot)
            if not zOld:
                zOld = ""

            zBuf = zOld
            if psp.insertLineMacro:
                if zBuf and zBuf[-1] != '\n':
                    zBuf += '\n'
                zBuf += "#line %d " % psp.tokenlineno
                zBuf += '"'
                zBuf += psp.filename.replace('\\', '\\\\')
                zBuf += '"'
                zBuf += '\n'

            zBuf += zNew
            setattr(psp.gp, psp.declargslot, zBuf)

            psp.state = WAITING_FOR_DECL_OR_RULE

        else:
            ErrorMsg(psp.filename, psp.tokenlineno,
                     "Illegal argument to %%%s: %s",
                     psp.declkeyword, x)
            psp.errorcnt += 1
            psp.state = RESYNC_AFTER_DECL_ERROR

    elif psp.state == WAITING_FOR_FALLBACK_ID:
        if x[0] == '.':
            psp.state = WAITING_FOR_DECL_OR_RULE
        elif not x[0].isupper():
            ErrorMsg(psp.filename, psp.tokenlineno,
                     '%%fallback argument "%s" should be a token', x)
            psp.errorcnt += 1
        else:
            sp = Symbol_new(x)
            if psp.fallback is None:
                psp.fallback = sp
            elif sp.fallback:
                ErrorMsg(psp.filename, psp.tokenlineno,
                         "More than one fallback assigned to token %s", x)
                psp.errorcnt += 1
            else:
                sp.fallback = psp.fallback
                psp.gp.has_fallback = 1

    elif psp.state == WAITING_FOR_WILDCARD_ID:
        if x[0] == '.':
            psp.state = WAITING_FOR_DECL_OR_RULE
        elif not x[0].isupper():
            ErrorMsg(psp.filename, psp.tokenlineno,
                     '%%wildcard argument "%s" should be a token', x)
            psp.errorcnt += 1
        else:
            sp = Symbol_new(x)
            if psp.gp.wildcard is None:
                psp.gp.wildcard = sp
            else:
                ErrorMsg(psp.filename, psp.tokenlineno, "Extra wildcard to token: %s", x)
                psp.errorcnt += 1

    elif psp.state in (RESYNC_AFTER_RULE_ERROR, RESYNC_AFTER_DECL_ERROR):
        if x[0] == '.':
            psp.state = WAITING_FOR_DECL_OR_RULE
        elif x[0] == '%':
            psp.state = WAITING_FOR_DECL_KEYWORD

    return


# In spite of its name, this function is really a scanner.  It read in
# the entire input file (all at once) then tokenizes it.  Each token
# is passed to the function "parseonetoken" which builds all the
# appropriate data structures in the global state vector "gp".

def Parse(gp):
    startline = 0

    ps = pstate(
        gp = gp,
        filename = gp.filename,
        errorcnt = 0,
        state = INITIALIZE,
        tokenlineno = 0,
        tokenstart = None,
        fallback = None,
        lhs = None,
        lhsalias = None,
        nrhs = 0,
        rhs = [None] * MAXRHS,
        alias = [None] * MAXRHS,
        prevrule = None,
        declkeyword = None,
        declargslot = None,
        insertLineMacro = False,
        declassoc = 0,
        preccounter = 0,
        firstrule = None,
        lastrule = None,
        )

    # Begin by reading the input file
    try:
        fp = open(ps.filename, "rb")
    except IOError:
        ErrorMsg(ps.filename, 0, "Can't open this file for reading.")
        gp.errorcnt += 1
        return
    filebuf = fp.read()
    fp.close()

    lineno = 1


    # Now scan the text of the input file

    cp = 0
    while cp < len(filebuf):
        c = filebuf[cp]

        # Keep track of the line number
        if c == '\n':
            lineno += 1

        # Skip all white space
        if c.isspace():
            cp += 1
            continue

        # Skip C++ style comments
        if filebuf[cp:cp+2] == "//":
            cp += 2
            while cp < len(filebuf):
                if filebuf[cp] == '\n':
                    break
                cp += 1
            continue

        # Skip C style comments
        if filebuf[cp:cp+2] == "/*":
            cp += 2
            while cp < len(filebuf):
                if filebuf[cp] == '\n':
                    lineno += 1
                if filebuf[cp-1:cp+1] == '*/':
                    cp += 1
                    break
                cp += 1
            continue

        ps.tokenstart = cp         # Mark the beginning of the token
        ps.tokenlineno = lineno    # Linenumber on which token begins

        if c == '"':
            # String literals
            cp += 1
            while cp < len(filebuf):
                c = filebuf[cp]
                if c == '"':
                    nextcp = cp + 1
                    break
                if c == '\n':
                    lineno += 1
                cp += 1
            else:
                ErrorMsg(ps.filename, startline,
                         "String starting on this line is not terminated "
                         "before the end of the file.")
                ps.errorcnt += 1
                nextcp = cp

        elif c == '{':
            # A block of C code
            cp += 1
            level = 1
            while cp < len(filebuf) and (level > 1 or filebuf[cp] != '}'):
                c = filebuf[cp]
                if c == '\n':
                    lineno += 1
                elif c == '{':
                    level += 1
                elif c == '}':
                    level -= 1

                elif filebuf[cp:cp+2] == "/*":
                    # Skip comments
                    cp += 2
                    while cp < len(filebuf):
                        c = filebuf[cp]
                        if filebuf[cp] == '\n':
                            lineno += 1
                        if filebuf[cp-1:cp+1] == '*/':
                            cp += 1
                            break
                        cp += 1

                elif filebuf[cp:cp+2] == "//":
                    # Skip C++ style comments too
                    cp += 2
                    while cp < len(filebuf):
                        if filebuf[cp] == '\n':
                            lineno += 1
                            break
                        cp += 1

                elif c == "'" or c == '"':
                    # String and character literals
                    startchar = c
                    prevc = 0
                    cp += 1
                    while (cp < len(filebuf) and
                           (filebuf[cp] != startchar or prevc == '\\')
                           ):
                        c = filebuf[cp]
                        if c == '\n':
                            lineno += 1
                        if prevc == '\\':
                            prevc = 0
                        else:
                            prevc = c
                        cp += 1

                cp += 1

            if cp == len(filebuf):
                ErrorMsg(ps.filename, ps.tokenlineno,
                         "C code starting on this line is not terminated "
                         "before the end of the file.")
                ps.errorcnt += 1
                nextcp = cp
            else:
                nextcp = cp + 1

        elif c.isalnum():
            # Identifiers
            while c.isalnum() or c == '_':
                cp += 1
                if cp > len(filebuf):
                    break
                c = filebuf[cp]
            nextcp = cp

        elif filebuf[cp:cp+3] == "::=":
            # The operator "::="
            cp += 3
            nextcp = cp

        elif (c in ('/', '|')) and cp+1 < len(filebuf) and filebuf[cp+1].isalpha():
            cp += 2
            while cp < len(filebuf):
                c = filebuf[cp]
                if not (c.isalnum() or c == '_'):
                    break
                cp += 1
            nextcp = cp

        else:
            # All other (one character) operators
            cp += 1
            nextcp = cp

        # Parse the token
        token = filebuf[ps.tokenstart:cp]
        parseonetoken(ps, token)

        cp = nextcp

    gp.rule = ps.firstrule
    gp.errorcnt = ps.errorcnt

    return
