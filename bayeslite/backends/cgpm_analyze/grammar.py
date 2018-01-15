# -*- Python -*-

# Driver template for the LEMON parser generator.
# The author disclaims copyright to this source code.


# First off, define the token values.  These constants (all generated
# automatically by the parser generator) specify the various kinds of
# tokens (terminals) that the parser understands.
#
# Each symbol here is a terminal symbol in the grammar.

T_SEMI                         =  1
K_VARIABLES                    =  2
K_SKIP                         =  3
K_ROWS                         =  4
K_LOOM                         =  5
K_OPTIMIZED                    =  6
K_QUIET                        =  7
K_SUBPROBLEM                   =  8
K_SUBPROBLEMS                  =  9
T_LROUND                       = 10
T_RROUND                       = 11
T_COMMA                        = 12
K_VARIABLE                     = 13
K_HYPERPARAMETERS              = 14
K_CLUSTERING                   = 15
K_CONCENTRATION                = 16
K_ROW                          = 17
L_NAME                         = 18
L_NUMBER                       = 19

# The state of the parser is completely contained in an instance of
# the following class.

class Parser(object):

    # defaults
    YYERRORSYMBOL = None
    YYWILDCARD = None

    # The next thing included is series of definitions which control
    # various aspects of the generated parser.
    #    YYNOCODE           is a number which corresponds
    #                       to no legal terminal or nonterminal number.  This
    #                       number is used to fill in empty slots of the hash 
    #                       table.
    #    YYNSTATE           the combined number of states.
    #    YYNRULE            the number of rules in the grammar
    #    YYERRORSYMBOL      is the code number of the error symbol.  If not
    #                       defined, then do no error processing.

    YYNOCODE = 32
    YYNSTATE = 38
    YYNRULE = 26
    YY_NO_ACTION     = YYNSTATE + YYNRULE + 2
    YY_ACCEPT_ACTION = YYNSTATE + YYNRULE + 1
    YY_ERROR_ACTION  = YYNSTATE + YYNRULE


    # Next are that tables used to determine what action to take based on the
    # current state and lookahead token.  These tables are used to implement
    # functions that take a state number and lookahead value and return an
    # action integer.  
    #
    # Suppose the action integer is N.  Then the action is determined as
    # follows
    #
    #   0 <= N < YYNSTATE                  Shift N.  That is, push the lookahead
    #                                      token onto the stack and goto state N.
    #
    #   YYNSTATE <= N < YYNSTATE+YYNRULE   Reduce by rule N-YYNSTATE.
    #
    #   N == YYNSTATE+YYNRULE              A syntax error has occurred.
    #
    #   N == YYNSTATE+YYNRULE+1            The parser accepts its input.
    #
    #   N == YYNSTATE+YYNRULE+2            No such action.  Denotes unused
    #                                      slots in the yy_action[] table.
    #
    # The action table is constructed as a single large table named yy_action[].
    # Given state S and lookahead X, the action is computed as
    #
    #      yy_action[ yy_shift_ofst[S] + X ]
    #
    # If the index value yy_shift_ofst[S]+X is out of range or if the value
    # yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
    # is equal to YY_SHIFT_USE_DFLT, it means that the action is not in the table
    # and that yy_default[S] should be used instead.  
    #
    # The formula above is for computing the action when the lookahead is
    # a terminal symbol.  If the lookahead is a non-terminal (as occurs after
    # a reduce action) then the yy_reduce_ofst[] array is used in place of
    # the yy_shift_ofst[] array and YY_REDUCE_USE_DFLT is used in place of
    # YY_SHIFT_USE_DFLT.
    #
    # The following are the tables generated in this section:
    #
    #  yy_action[]        A single table containing all actions.
    #  yy_lookahead[]     A table containing the lookahead for each entry in
    #                     yy_action.  Used to detect hash collisions.
    #  yy_shift_ofst[]    For each state, the offset into yy_action for
    #                     shifting terminals.
    #  yy_reduce_ofst[]   For each state, the offset into yy_action for
    #                     shifting non-terminals after a reduce.
    #  yy_default[]       Default action for each state.

    YY_ACTTAB_COUNT = 46
    yy_action = [
            5,    4,    3,   30,   29,   28,    1,    1,    9,    2, #     0
           38,    6,   10,   65,   12,   19,   14,   25,    7,   23, #    10
           15,   10,   16,   36,   18,   14,   17,   31,    8,   34, #    20
           22,   34,   13,   35,   20,   11,   21,   27,   26,   33, #    30
           32,   66,   37,   66,   66,   24, #    40
        ]
    yy_lookahead = [
            2,    3,    4,    5,    6,    7,    8,    9,   12,   10, #     0
            0,    1,   13,   21,   22,   23,   17,   11,   12,   14, #    10
           15,   13,   25,   29,   24,   17,   24,   30,   12,   29, #    20
           16,   29,   15,   18,   27,   28,   16,   26,   27,   30, #    30
           19,   31,   23,   31,   31,   27, #    40
        ]
    YY_SHIFT_USE_DFLT = -5
    YY_SHIFT_COUNT = 18
    YY_SHIFT_MIN = -4
    YY_SHIFT_MAX = 21
    yy_shift_ofst = [
           -2,   -1,    8,   21,   15,   15,   -2,    8,   21,   15, #     0
            5,    6,   10,   20,   17,   14,   16,   -4,   -4, #    10
        ]
    YY_REDUCE_USE_DFLT = -9
    YY_REDUCE_COUNT = 9
    YY_REDUCE_MIN = -8
    YY_REDUCE_MAX = 19
    yy_reduce_ofst = [
           -8,   11,    7,   -3,    2,    0,   19,   18,    9,   -6, #     0
        ]
    yy_default = [
           41,   64,   64,   64,   64,   64,   41,   64,   64,   64, #     0
           64,   64,   64,   56,   64,   54,   44,   43,   42,   39, #    10
           51,   57,   55,   53,   52,   50,   49,   48,   47,   46, #    20
           45,   61,   63,   62,   58,   60,   59,   40, #    30
        ]


    # The next table maps tokens into fallback tokens.  If a construct
    # like the following:
    #
    #      %fallback ID X Y Z.
    #
    # appears in the grammer, then ID becomes a fallback token for X, Y,
    # and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
    # but it does not parse, the type of the token is changed to ID and
    # the parse is retried before an error is thrown.

    yyFallback = [
        ]


    # The following structure represents a single element of the
    # parser's stack.  Information stored includes:
    #
    #   +  The state number for the parser at this level of the stack.
    #
    #   +  The value of the token stored at this level of the stack.
    #      (In other words, the "major" token.)
    #
    #   +  The semantic value stored at this level of the stack.  This is
    #      the information used by the action routines in the grammar.
    #      It is sometimes called the "minor" token.
    #
    class yyStackEntry(object):
        def __init__(
            self,
            stateno, # The state-number
            major,   # The major token value.  This is the code
                     # number for the token at this stack level
            minor,   # The user-supplied minor token value.  This
                     # is the value of the token
            ):
            self.stateno = stateno
            self.major = major
            self.minor = minor
            return


    yyTraceFILE = None
    yyTracePrompt = None

    def trace(self, TraceFILE, zTracePrompt):
        '''Turn parser tracing on by giving a stream to which to write
        the trace and a prompt to preface each trace message.  Tracing
        is turned off by making either argument None.
        '''
        self.yyTraceFILE = TraceFILE
        self.yyTracePrompt = zTracePrompt
        if self.yyTraceFILE is None:
            self.yyTracePrompt = None
        elif self.yyTracePrompt is None:
            self.yyTraceFILE = None
        return


    # For tracing shifts, the names of all terminals and nonterminals
    # are required.  The following table supplies these names
    yyTokenName = [
        "$",                   "T_SEMI",              "K_VARIABLES",         "K_SKIP",      
        "K_ROWS",              "K_LOOM",              "K_OPTIMIZED",         "K_QUIET",     
        "K_SUBPROBLEM",        "K_SUBPROBLEMS",        "T_LROUND",            "T_RROUND",    
        "T_COMMA",             "K_VARIABLE",          "K_HYPERPARAMETERS",        "K_CLUSTERING",
        "K_CONCENTRATION",        "K_ROW",               "L_NAME",              "L_NUMBER",    
        "error",               "analysis",            "phrases",             "phrase",      
        "column_list",         "row_list",            "subproblems_list",        "subproblem",  
        "subproblems",         "column_name",         "row_index",   
        ]

    # For tracing reduce actions, the names of all rules are required.
    yyRuleName = [
        "analysis ::= phrases", #   0
        "phrases ::= phrase", #   1
        "phrases ::= phrases T_SEMI phrase", #   2
        "phrase ::=", #   3
        "phrase ::= K_VARIABLES column_list", #   4
        "phrase ::= K_SKIP column_list", #   5
        "phrase ::= K_ROWS row_list", #   6
        "phrase ::= K_LOOM", #   7
        "phrase ::= K_OPTIMIZED", #   8
        "phrase ::= K_QUIET", #   9
        "phrase ::= K_SUBPROBLEM|K_SUBPROBLEMS subproblems_list", #  10
        "subproblems_list ::= subproblem", #  11
        "subproblems_list ::= T_LROUND subproblems T_RROUND", #  12
        "subproblems ::= subproblem", #  13
        "subproblems ::= subproblems T_COMMA subproblem", #  14
        "subproblem ::= K_VARIABLE K_HYPERPARAMETERS", #  15
        "subproblem ::= K_VARIABLE K_CLUSTERING", #  16
        "subproblem ::= K_VARIABLE K_CLUSTERING K_CONCENTRATION", #  17
        "subproblem ::= K_ROW K_CLUSTERING", #  18
        "subproblem ::= K_ROW K_CLUSTERING K_CONCENTRATION", #  19
        "column_list ::= column_name", #  20
        "column_list ::= column_list T_COMMA column_name", #  21
        "column_name ::= L_NAME", #  22
        "row_list ::= row_index", #  23
        "row_list ::= row_list T_COMMA row_index", #  24
        "row_index ::= L_NUMBER", #  25
        ]


    def __init__(self, delegate):
        self.yystack = [] # The parser's stack
        self.delegate = delegate
        return


    def yy_pop_parser_stack(self):
        """Pop the parser's stack once. Return the major token number
        for the symbol popped.
        """
        if not self.yystack:
            return 0
        yytos = self.yystack.pop()
        if self.yyTraceFILE:
            self.yyTraceFILE.write("%sPopping %s\n" % (
                self.yyTracePrompt,
                self.yyTokenName[yytos.major]))
        yymajor = yytos.major
        return yymajor


    def yy_find_shift_action(self,       # The parser
                             iLookAhead  # The look-ahead token
                             ):
        '''Find the appropriate action for a parser given the terminal
        look-ahead token iLookAhead.

        If the look-ahead token is YYNOCODE, then check to see if the
        action is independent of the look-ahead.  If it is, return the
        action, otherwise return YY_NO_ACTION.
        '''
        yyTraceFILE = self.yyTraceFILE
        stateno = self.yystack[-1].stateno
        if stateno > self.YY_SHIFT_COUNT:
            return self.yy_default[stateno]
        i = self.yy_shift_ofst[stateno]
        if i == self.YY_SHIFT_USE_DFLT:
            return self.yy_default[stateno]
        assert iLookAhead != self.YYNOCODE
        i += iLookAhead
        if i < 0 or i >= self.YY_ACTTAB_COUNT or self.yy_lookahead[i] != iLookAhead:
            if iLookAhead > 0:
                yyFallback = self.yyFallback
                yyTokenName = self.yyTokenName
                if iLookAhead < len(yyFallback):
                    iFallback = yyFallback[iLookAhead] # Fallback token
                    if iFallback != 0:
                        if yyTraceFILE:
                            yyTraceFILE.write(
                                "%sFALLBACK %s => %s\n" %
                                (self.yyTracePrompt,
                                 yyTokenName[iLookAhead], yyTokenName[iFallback]))
                        return self.yy_find_shift_action(iFallback);
                YYWILDCARD = self.YYWILDCARD
                if YYWILDCARD is not None:
                    j = i - iLookAhead + YYWILDCARD
                    # [TRC 20150122: Lemon avoids generating tests
                    # against j if they can never fail, based on
                    # compile-time values here, presumably to limit
                    # compiler warnings.  Not so easy to do in Python
                    # so we'll just skip that.]
                    if j >= 0 and j < self.YY_ACTTAB_COUNT and self.yy_lookahead[j] == YYWILDCARD:
                        if yyTraceFILE:
                            yyTraceFILE.write(
                                "%sWILDCARD %s => %s\n" %
                                (self.yyTracePrompt,
                                 yyTokenName[iLookAhead], yyTokenName[YYWILDCARD]))
                        return self.yy_action[j];
            return self.yy_default[stateno]
        else:
            return self.yy_action[i]


    def yy_find_reduce_action(self,
                              stateno,    # Current state number
                              iLookAhead  # The look-ahead token
                              ):
        '''Find the appropriate action for a parser given the
        non-terminal look-ahead token iLookAhead.
        
        If the look-ahead token is YYNOCODE, then check to see if the
        action is independent of the look-ahead.  If it is, return the
        action, otherwise return YY_NO_ACTION.
        '''
        YYERRORSYMBOL = self.YYERRORSYMBOL
        if YYERRORSYMBOL is not None:
            if stateno > self.YY_REDUCE_COUNT:
                return self.yy_default[stateno]
        else:
            assert stateno <= self.YY_REDUCE_COUNT
        i = self.yy_reduce_ofst[stateno]
        assert i != self.YY_REDUCE_USE_DFLT
        assert iLookAhead != self.YYNOCODE
        i += iLookAhead
        if YYERRORSYMBOL is not None:
            if i < 0 or i >= self.YY_ACTTAB_COUNT or self.yy_lookahead[i] != iLookAhead:
                return self.yy_default[stateno]
        else:
            assert i >= 0 and i < self.YY_ACTTAB_COUNT
            assert self.yy_lookahead[i] == iLookAhead
        return self.yy_action[i]


    def yy_shift(self,        # The parser to be shifted
                 yyNewState,  # The new state to shift in
                 yyMajor,     # The major token to shift in
                 yyMinor      # The minor token to shift in
                 ):
        '''Perform a shift action.'''

        yytos = self.yyStackEntry(
            stateno = yyNewState,
            major = yyMajor,
            minor = yyMinor
            )
        self.yystack.append(yytos)

        yyTraceFILE = self.yyTraceFILE
        if yyTraceFILE:
            yyTraceFILE.write("%sShift %d\n" % (self.yyTracePrompt, yyNewState))
            yyTraceFILE.write("%sStack:" % self.yyTracePrompt)
            for entry in self.yystack:
                yyTraceFILE.write(" %s" % self.yyTokenName[entry.major])
            yyTraceFILE.write("\n")

        return


    # The following table contains information about every rule that
    # is used during the reduce.
    from collections import namedtuple
    yyRuleInfoEntry = namedtuple(
        'yyRuleInfoEntry',
        ('lhs',  # Symbol on the left-hand side of the rule
         'nrhs', # Number of right-hand side symbols in the rule
         ))
    yyRuleInfo = [
        yyRuleInfoEntry( 21, 1 ),
        yyRuleInfoEntry( 22, 1 ),
        yyRuleInfoEntry( 22, 3 ),
        yyRuleInfoEntry( 23, 0 ),
        yyRuleInfoEntry( 23, 2 ),
        yyRuleInfoEntry( 23, 2 ),
        yyRuleInfoEntry( 23, 2 ),
        yyRuleInfoEntry( 23, 1 ),
        yyRuleInfoEntry( 23, 1 ),
        yyRuleInfoEntry( 23, 1 ),
        yyRuleInfoEntry( 23, 2 ),
        yyRuleInfoEntry( 26, 1 ),
        yyRuleInfoEntry( 26, 3 ),
        yyRuleInfoEntry( 28, 1 ),
        yyRuleInfoEntry( 28, 3 ),
        yyRuleInfoEntry( 27, 2 ),
        yyRuleInfoEntry( 27, 2 ),
        yyRuleInfoEntry( 27, 3 ),
        yyRuleInfoEntry( 27, 2 ),
        yyRuleInfoEntry( 27, 3 ),
        yyRuleInfoEntry( 24, 1 ),
        yyRuleInfoEntry( 24, 3 ),
        yyRuleInfoEntry( 29, 1 ),
        yyRuleInfoEntry( 25, 1 ),
        yyRuleInfoEntry( 25, 3 ),
        yyRuleInfoEntry( 30, 1 ),
        ]


    # Action code for each rule follows.
    def action_000(self):
        # analysis ::= phrases
        return self.delegate.p_analysis_start(
            ps = self.yystack[-1].minor,
            )
    def action_001(self):
        # phrases ::= phrase
        return self.delegate.p_phrases_one(
            p = self.yystack[-1].minor,
            )
    def action_002(self):
        # phrases ::= phrases T_SEMI phrase
        return self.delegate.p_phrases_many(
            ps = self.yystack[-3].minor,
            p = self.yystack[-1].minor,
            )
    def action_003(self):
        # phrase ::=
        return self.delegate.p_phrase_none(
            )
    def action_004(self):
        # phrase ::= K_VARIABLES column_list
        return self.delegate.p_phrase_variables(
            cols = self.yystack[-1].minor,
            )
    def action_005(self):
        # phrase ::= K_SKIP column_list
        return self.delegate.p_phrase_skip(
            cols = self.yystack[-1].minor,
            )
    def action_006(self):
        # phrase ::= K_ROWS row_list
        return self.delegate.p_phrase_rows(
            rows = self.yystack[-1].minor,
            )
    def action_007(self):
        # phrase ::= K_LOOM
        return self.delegate.p_phrase_loom(
            )
    def action_008(self):
        # phrase ::= K_OPTIMIZED
        return self.delegate.p_phrase_optimized(
            )
    def action_009(self):
        # phrase ::= K_QUIET
        return self.delegate.p_phrase_quiet(
            )
    def action_010(self):
        # phrase ::= K_SUBPROBLEM|K_SUBPROBLEMS subproblems_list
        return self.delegate.p_phrase_subproblems(
            s = self.yystack[-1].minor,
            )
    def action_011(self):
        # subproblems_list ::= subproblem
        return self.delegate.p_subproblems_list_one(
            s = self.yystack[-1].minor,
            )
    def action_012(self):
        # subproblems_list ::= T_LROUND subproblems T_RROUND
        return self.delegate.p_subproblems_list_many(
            s = self.yystack[-2].minor,
            )
    def action_013(self):
        # subproblems ::= subproblem
        return self.delegate.p_subproblems_one(
            s = self.yystack[-1].minor,
            )
    def action_014(self):
        # subproblems ::= subproblems T_COMMA subproblem
        return self.delegate.p_subproblems_many(
            ss = self.yystack[-3].minor,
            s = self.yystack[-1].minor,
            )
    def action_015(self):
        # subproblem ::= K_VARIABLE K_HYPERPARAMETERS
        return self.delegate.p_subproblem_variable_hyperparameters(
            )
    def action_016(self):
        # subproblem ::= K_VARIABLE K_CLUSTERING
        return self.delegate.p_subproblem_variable_clustering(
            )
    def action_017(self):
        # subproblem ::= K_VARIABLE K_CLUSTERING K_CONCENTRATION
        return self.delegate.p_subproblem_variable_clustering_concentration(
            )
    def action_018(self):
        # subproblem ::= K_ROW K_CLUSTERING
        return self.delegate.p_subproblem_row_clustering(
            )
    def action_019(self):
        # subproblem ::= K_ROW K_CLUSTERING K_CONCENTRATION
        return self.delegate.p_subproblem_row_clustering_concentration(
            )
    def action_020(self):
        # column_list ::= column_name
        return self.delegate.p_column_list_one(
            col = self.yystack[-1].minor,
            )
    def action_021(self):
        # column_list ::= column_list T_COMMA column_name
        return self.delegate.p_column_list_many(
            cols = self.yystack[-3].minor,
            col = self.yystack[-1].minor,
            )
    def action_022(self):
        # column_name ::= L_NAME
        return self.delegate.p_column_name_n(
            name = self.yystack[-1].minor,
            )
    def action_023(self):
        # row_list ::= row_index
        return self.delegate.p_row_list_one(
            row = self.yystack[-1].minor,
            )
    def action_024(self):
        # row_list ::= row_list T_COMMA row_index
        return self.delegate.p_row_list_many(
            rows = self.yystack[-3].minor,
            row = self.yystack[-1].minor,
            )
    def action_025(self):
        # row_index ::= L_NUMBER
        return self.delegate.p_row_index_n(
            n = self.yystack[-1].minor,
            )
    yy_action_method = [
        action_000,
        action_001,
        action_002,
        action_003,
        action_004,
        action_005,
        action_006,
        action_007,
        action_008,
        action_009,
        action_010,
        action_011,
        action_012,
        action_013,
        action_014,
        action_015,
        action_016,
        action_017,
        action_018,
        action_019,
        action_020,
        action_021,
        action_022,
        action_023,
        action_024,
        action_025,
    ]


    def yy_reduce(self,     # The parser
                  yyruleno  # Number of the rule by which to reduce
                  ):
        '''Perform a reduce action and the shift that must immediately
        follow the reduce.'''
        
        if (self.yyTraceFILE and
            yyruleno >= 0 and yyruleno < len(self.yyRuleName)
            ):
            self.yyTraceFILE.write("%sReduce [%s].\n" % (
                self.yyTracePrompt, self.yyRuleName[yyruleno]))

        # get the action
        action = self.yy_action_method[yyruleno]

        # 'yygotominor' is the LHS of the rule reduced
        yygotominor = action(self)

        yygoto = self.yyRuleInfo[yyruleno].lhs   # The next state
        yysize = self.yyRuleInfo[yyruleno].nrhs  # Amount to pop the stack
        if yysize > 0:
            del self.yystack[-yysize:]

        # The next action
        yyact = self.yy_find_reduce_action(self.yystack[-1].stateno, yygoto)

        if yyact < self.YYNSTATE:
            self.yy_shift(yyact, yygoto, yygotominor)
        else:
            assert yyact == self.YYNSTATE + self.YYNRULE + 1
            self.yy_accept()

        return


    def yy_parse_failed(self):
        '''This method executes when the parse fails.'''

        if self.yyTraceFILE:
            self.yyTraceFILE.write("%sFail!\n" % self.yyTracePrompt)

        while self.yystack:
            self.yy_pop_parser_stack()

        self.delegate.parse_failed()

        return


    def yy_syntax_error(self, token):
        '''This method executes when a syntax error occurs.'''
        self.delegate.syntax_error(token)
        return


    def yy_accept(self):
        '''This method executes when the parser accepts.'''

        if self.yyTraceFILE:
            self.yyTraceFILE.write("%sAccept!\n" % self.yyTracePrompt)

        while self.yystack:
            self.yy_pop_parser_stack()

        self.delegate.accept()

        return


    def parse(self, tokens):
        for token in tokens:
            self.feed(token)
        self.feed((0, None))
        return


    def feed(self, token):
        '''The main parser routine.'''

        yymajor = token[0]  # The major token code number
        yyminor = token[1]  # The value for the token

        yyerrorhit = False  # True if yymajor has invoked an error

        # (re)initialize the parser, if necessary
        if not self.yystack:
            self.yyerrcnt = -1
            yytos = self.yyStackEntry(
                stateno = 0,
                major = 0,
                minor = None
                )
            self.yystack.append(yytos)

        yyendofinput = (yymajor == 0) # True if we are at the end of input
        
        if self.yyTraceFILE:
            self.yyTraceFILE.write(
                "%sInput %s\n" %
                (self.yyTracePrompt, self.yyTokenName[yymajor]))


        cond = True
        while cond:

            # The parser action.
            yyact = self.yy_find_shift_action(yymajor)

            YYNOCODE = self.YYNOCODE
            YYNSTATE = self.YYNSTATE
            YYNRULE  = self.YYNRULE

            if yyact < YYNSTATE:
                assert not yyendofinput, "Impossible to shift the $ token"
                self.yy_shift(yyact, yymajor, yyminor)
                self.yyerrcnt -= 1
                yymajor = YYNOCODE
            elif yyact < YYNSTATE + YYNRULE:
                self.yy_reduce(yyact - YYNSTATE)
            else:
                assert yyact == self.YY_ERROR_ACTION
                if self.yyTraceFILE:
                    self.yyTraceFILE.write(
                        "%sSyntax Error!\n" % self.yyTracePrompt)

                YYERRORSYMBOL = self.YYERRORSYMBOL
                if YYERRORSYMBOL is not None:
                    # A syntax error has occurred.
                    # The response to an error depends upon whether or not the
                    # grammar defines an error token "ERROR".  
                    #
                    # This is what we do if the grammar does define ERROR:
                    #
                    #  * Call the %syntax_error function.
                    #
                    #  * Begin popping the stack until we enter a state where
                    #    it is legal to shift the error symbol, then shift
                    #    the error symbol.
                    #
                    #  * Set the error count to three.
                    #
                    #  * Begin accepting and shifting new tokens.  No new error
                    #    processing will occur until three tokens have been
                    #    shifted successfully.
                    #
                    if self.yyerrcnt < 0:
                        self.yy_syntax_error(token)

                    yymx = self.yystack[-1].major
                    if yymx == YYERRORSYMBOL or yyerrorhit:
                        if self.yyTraceFILE:
                            self.yyTraceFILE.write(
                                "%sDiscard input token %s\n" % (
                                    self.yyTracePrompt,
                                    self.yyTokenName[yymajor]))
                        yymajor = YYNOCODE
                    else:
                        while self.yystack and yymx != YYERRORSYMBOL:
                            yyact = self.yy_find_reduce_action(
                                self.yystack[-1].stateno,
                                YYERRORSYMBOL
                                )
                            if yyact < YYNSTATE:
                                break
                            self.yy_pop_parser_stack()

                        if not self.yystack or yymajor == 0:
                            self.yy_parse_failed()
                            yymajor = YYNOCODE
                        elif yymx != YYERRORSYMBOL:
                            self.yy_shift(yyact, YYERRORSYMBOL, None)

                    self.yyerrcnt = 3
                    yyerrorhit = True

                else: # YYERRORSYMBOL is not defined
                    # This is what we do if the grammar does not define ERROR:
                    #
                    #  * Report an error message, and throw away the input token.
                    #
                    #  * If the input token is $, then fail the parse.
                    #
                    # As before, subsequent error messages are suppressed until
                    # three input tokens have been successfully shifted.
                    #
                    if self.yyerrcnt <= 0:
                        self.yy_syntax_error(token)

                    self.yyerrcnt = 3
                    if yyendofinput:
                        self.yy_parse_failed()

                    yymajor = YYNOCODE

            cond = yymajor != YYNOCODE and self.yystack

        return


