'''
Principal data structures for the LEMON parser generator.
'''


from ccruft import struct



# Symbols (terminals and nonterminals) of the grammar are stored in
# the following:

( # type
    TERMINAL,
    NONTERMINAL,
    MULTITERMINAL,
    ) = range(3)

( # assoc
    LEFT,
    RIGHT,
    NONE,
    UNK,
    ) = range(4)

symbol = struct(
    'symbol',
    (
        'name',         # Name of the symbol
        'index',        # Index number for this symbol
        'type',         # Symbols are all either TERMINALS or NTs
        'rule',         # Linked list of rules of this (if an NT)
        'fallback',     # fallback token in case this token doesn't parse
        'prec',         # Precedence if defined (-1 otherwise)
        'assoc',        # Associativity if predecence is defined
        'firstset',     # First-set for all rules of this symbol
        '_lambda',      # True if NT and can generate an empty string
        'useCnt',       # Number of times used
        # The following fields are used by MULTITERMINALs only
        'nsubsym',      # Number of constituent symbols in the MULTI
        'subsym',       # Array of constituent symbols
        )
    )



# Each production rule in the grammar is stored in the following
# structure.

rule = struct(
    'rule',
    (
        'lhs',          # Left-hand side of the rule
        'lhsalias',     # Alias for the LHS (NULL if none)
        'lhsStart',     # True if left-hand side is the start symbol
        'ruleline',     # Line number for the rule
        'nrhs',         # Number of RHS symbols
        'rhs',          # The RHS symbols
        'rhsalias',     # An alias for each RHS symbol (NULL if none)
        'line',         # Line number at which code begins
        'code',         # The code executed when this rule is reduced
        'precsym',      # Precedence symbol for this rule
        'index',        # An index number for this rule
        'canReduce',    # True if this rule is ever reduced
        'nextlhs',      # Next rule with the same LHS
        'next',         # Next rule in the global list
        )
    )



# A configuration is a production rule of the grammar together with a
# mark (dot) showing how much of that rule has been processed so far.
# Configurations also contain a follow-set which is a list of terminal
# symbols which are allowed to immediately follow the end of the rule.
# Every configuration is recorded as an instance of the following:

( # status
    COMPLETE,
    INCOMPLETE
) = range(2)

config = struct(
    'config',
    (
        'rp',           # The rule upon which the configuration is based
        'dot',          # The parse point
        'fws',          # Follow-set for this configuration only
        'fplp',         # Follow-set forward propagation links
        'bplp',         # Follow-set backwards propagation links
        'stp',          # Pointer to state which contains this
        'status',       # The status is used during followset and shift computations
        'next',         # Next configuration in the state
        'bp',           # The next basis configuration
        )
    )



# Every shift or reduce operation is stored as one of the following

( # type
    SHIFT,
    ACCEPT,
    REDUCE,
    ERROR,
    SSCONFLICT,         # A shift/shift conflict
    SRCONFLICT,         # Was a reduce, but part of a conflict
    RRCONFLICT,         # Was a reduce, but part of a conflict
    SH_RESOLVED,        # Was a shift.  Precedence resolved conflict
    RD_RESOLVED,        # Was reduce.  Precedence resolved conflict
    NOT_USED,           # Deleted by compression
) = range(10)

action = struct(
    'action',
    (
        'sp',           # The look-ahead symbol
        'type',
        'stp',          # The new state, if a shift
        'rp',           # The rule, if a reduce
        'next',         # Next action for this state
        'collide',      # Next action with the same hash
        )
    )
action.x = property(lambda self: self) # union



# Each state of the generated parser's finite state machine is encoded
# as an instance of the following structure.

state = struct(
    'state',
    (
        'bp',           # The basis configurations for this state
        'cfp',          # All configurations in this set
        'statenum',     # Sequencial number for this state
        'ap',           # Array of actions for this state
        'nTknAct', 'nNtAct',      # Number of actions on terminals and nonterminals
        'iTknOfst', 'iNtOfst',    # yy_action[] offset for terminals and nonterms
        'iDflt',        # Default action
        )
    )

NO_OFFSET = -2147483647



# A followset propagation link indicates that the contents of one
# configuration followset should be propagated to another whenever the
# first changes.

plink = struct(
    'plink',
    (
        'cfp',          # The configuration to which linked
        'next',         # The next propagate link
        )
    )


# The state vector for the entire parser generator is recorded as
# follows.  (LEMON uses no global variables and makes little use of
# static variables.  Fields in the following structure can be thought
# of as begin global variables in the program.)

lemon = struct(
    'lemon',
    (
        'sorted',       # Table of states sorted by state number
        'rule',         # List of all rules
        'nstate',       # Number of states
        'nrule',        # Number of rules
        'nsymbol',      # Number of terminal and nonterminal symbols
        'nterminal',    # Number of terminal symbols
        'symbols',      # Sorted array of pointers to symbols
        'errorcnt',     # Number of errors
        'errsym',       # The error symbol
        'wildcard',     # Token that matches anything
        'name',         # Name of the generated parser
        'start',        # Name of the start symbol for the grammar
        'filename',     # Name of the input file
        'outname',      # Name of the current output file
        'tokenprefix',  # A prefix added to token names in the .h file
        'nconflict',    # Number of parsing conflicts
        'tablesize',    # Size of the parse tables
        'basisflag',    # Pr'only basis configurations
        'has_fallback', # True if any %fallback is seen in the grammer
        'argv0',        # Name of the program
        )
    )

