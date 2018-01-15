# -*- Python -*-

# Driver template for the LEMON parser generator.
# The author disclaims copyright to this source code.


# First off, define the token values.  These constants (all generated
# automatically by the parser generator) specify the various kinds of
# tokens (terminals) that the parser understands.
#
# Each symbol here is a terminal symbol in the grammar.

T_COMMA                        =  1
K_ENSURE                       =  2
K_IN                           =  3
K_OF                           =  4
K_SINGLETON                    =  5
K_SET                          =  6
K_CONCENTRATION                =  7
K_PARAMETER                    =  8
K_TO                           =  9
K_ROW                          = 10
K_ROWS                         = 11
K_CLUSTER                      = 12
K_WITHIN                       = 13
K_VARIABLE                     = 14
K_VARIABLES                    = 15
K_VIEW                         = 16
K_CONTEXT                      = 17
K_INDEPENDENT                  = 18
K_DEPENDENT                    = 19
T_STAR                         = 20
T_LROUND                       = 21
T_RROUND                       = 22
L_NAME                         = 23
L_NUMBER                       = 24

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

    YYNOCODE = 40
    YYNSTATE = 73
    YYNRULE = 31
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

    YY_ACTTAB_COUNT = 95
    yy_action = [
            1,    1,    7,   12,   50,   49,   32,   62,    4,   21, #     0
           69,   17,   67,   66,   67,   66,   22,   65,   64,   38, #    10
           54,    3,   73,   13,   57,   41,   42,  105,   26,   45, #    20
           61,    5,   53,   24,   20,    6,   67,   66,   59,   51, #    30
           11,   23,   25,    8,   43,   55,   63,   19,   37,   18, #    40
           10,   34,   31,   40,   33,   30,   16,   29,   70,   15, #    50
           14,   69,   52,    9,   60,   57,   58,   56,  106,   48, #    60
           72,  106,   27,  106,  106,  106,   47,  106,  106,  106, #    70
          106,  106,  106,  106,  106,  106,   68,   46,   39,   36, #    80
           28,   71,    2,   44,   35, #    90
        ]
    yy_lookahead = [
           10,   11,    3,    5,   14,   15,   10,   20,   21,    1, #     0
           23,    1,   16,   17,   16,   17,    4,   18,   19,    5, #    10
           20,   21,    0,    1,   24,    4,   12,   26,   27,   28, #    20
           22,    2,   22,    3,   10,    6,   16,   17,   33,   36, #    30
           13,   38,   37,   30,   35,   36,   33,    4,   12,    4, #    40
           13,    7,   12,   36,    8,    7,    9,    8,   33,    4, #    50
            9,   23,   36,   13,   33,   24,   33,   33,   39,   34, #    60
           28,   39,   33,   39,   39,   39,   24,   39,   39,   39, #    70
           39,   39,   39,   39,   39,   39,   32,   34,   32,   32, #    80
           32,   31,   29,   32,   32, #    90
        ]
    YY_SHIFT_USE_DFLT = -14
    YY_SHIFT_COUNT = 44
    YY_SHIFT_MIN = -13
    YY_SHIFT_MAX = 55
    yy_shift_ofst = [
           29,    0,  -13,   41,   38,  -10,   -4,   -2,   -1,   20, #     0
           20,   20,   20,   29,   52,   38,   52,   41,   38,   38, #    10
           41,   38,   38,   10,   14,    8,   22,   51,   55,   50, #    20
           49,   48,   40,   47,   46,   44,   45,   37,   36,   43, #    30
           27,   24,   21,   30,   12, #    40
        ]
    YY_REDUCE_USE_DFLT = -1
    YY_REDUCE_COUNT = 22
    YY_REDUCE_MIN = 0
    YY_REDUCE_MAX = 63
    yy_reduce_ofst = [
            1,    9,   13,    3,    5,   63,   62,   61,   60,   58, #     0
           57,   56,   54,   42,   53,   39,   35,   26,   34,   33, #    10
           17,   31,   25, #    20
        ]
    yy_default = [
           76,  104,  104,  104,  104,   84,  104,  104,  104,  104, #     0
          104,  104,  104,   76,  104,  104,  104,  104,  104,  104, #    10
          104,  104,  104,  104,  104,  104,  104,  104,  104,  104, #    20
          104,  104,  104,  104,  104,  104,  104,  104,  104,  104, #    30
          104,  104,  104,  104,  104,   74,   83,  103,   80,   86, #    40
           85,  100,  101,   99,   98,   97,   82,  102,   81,   94, #    50
           95,   93,   92,   91,   90,   89,   88,   87,   79,   96, #    60
           78,   77,   75, #    70
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
        "$",                   "T_COMMA",             "K_ENSURE",            "K_IN",        
        "K_OF",                "K_SINGLETON",         "K_SET",               "K_CONCENTRATION",
        "K_PARAMETER",         "K_TO",                "K_ROW",               "K_ROWS",      
        "K_CLUSTER",           "K_WITHIN",            "K_VARIABLE",          "K_VARIABLES", 
        "K_VIEW",              "K_CONTEXT",           "K_INDEPENDENT",        "K_DEPENDENT", 
        "T_STAR",              "T_LROUND",            "T_RROUND",            "L_NAME",      
        "L_NUMBER",            "error",               "alter",               "phrases",     
        "phrase",              "variable_token_opt",        "columns",             "dependency",  
        "view_token",          "column_name",         "concentration",        "rows",        
        "row_index",           "column_list",         "row_list",    
        ]

    # For tracing reduce actions, the names of all rules are required.
    yyRuleName = [
        "alter ::= phrases", #   0
        "phrases ::= phrase", #   1
        "phrases ::= phrases T_COMMA phrase", #   2
        "phrase ::=", #   3
        "phrase ::= K_ENSURE variable_token_opt columns dependency", #   4
        "phrase ::= K_ENSURE variable_token_opt columns K_IN view_token K_OF column_name", #   5
        "phrase ::= K_ENSURE variable_token_opt columns K_IN K_SINGLETON view_token", #   6
        "phrase ::= K_SET view_token K_CONCENTRATION K_PARAMETER K_TO concentration", #   7
        "phrase ::= K_ENSURE K_ROW|K_ROWS rows K_IN K_CLUSTER K_OF K_ROW row_index K_WITHIN view_token K_OF column_name", #   8
        "phrase ::= K_ENSURE K_ROW|K_ROWS rows K_IN K_SINGLETON K_CLUSTER K_WITHIN view_token K_OF column_name", #   9
        "phrase ::= K_SET K_ROW K_CLUSTER K_CONCENTRATION K_PARAMETER K_WITHIN view_token K_OF column_name K_TO concentration", #  10
        "variable_token_opt ::=", #  11
        "variable_token_opt ::= K_VARIABLE", #  12
        "variable_token_opt ::= K_VARIABLES", #  13
        "view_token ::= K_VIEW", #  14
        "view_token ::= K_CONTEXT", #  15
        "dependency ::= K_INDEPENDENT", #  16
        "dependency ::= K_DEPENDENT", #  17
        "columns ::= column_name", #  18
        "columns ::= T_STAR", #  19
        "columns ::= T_LROUND column_list T_RROUND", #  20
        "column_list ::= column_name", #  21
        "column_list ::= column_list T_COMMA column_name", #  22
        "column_name ::= L_NAME", #  23
        "rows ::= row_index", #  24
        "rows ::= T_STAR", #  25
        "rows ::= T_LROUND row_list T_RROUND", #  26
        "row_list ::= row_index", #  27
        "row_list ::= row_list T_COMMA row_index", #  28
        "row_index ::= L_NUMBER", #  29
        "concentration ::= L_NUMBER", #  30
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
        yyRuleInfoEntry( 26, 1 ),
        yyRuleInfoEntry( 27, 1 ),
        yyRuleInfoEntry( 27, 3 ),
        yyRuleInfoEntry( 28, 0 ),
        yyRuleInfoEntry( 28, 4 ),
        yyRuleInfoEntry( 28, 7 ),
        yyRuleInfoEntry( 28, 6 ),
        yyRuleInfoEntry( 28, 6 ),
        yyRuleInfoEntry( 28, 12 ),
        yyRuleInfoEntry( 28, 10 ),
        yyRuleInfoEntry( 28, 11 ),
        yyRuleInfoEntry( 29, 0 ),
        yyRuleInfoEntry( 29, 1 ),
        yyRuleInfoEntry( 29, 1 ),
        yyRuleInfoEntry( 32, 1 ),
        yyRuleInfoEntry( 32, 1 ),
        yyRuleInfoEntry( 31, 1 ),
        yyRuleInfoEntry( 31, 1 ),
        yyRuleInfoEntry( 30, 1 ),
        yyRuleInfoEntry( 30, 1 ),
        yyRuleInfoEntry( 30, 3 ),
        yyRuleInfoEntry( 37, 1 ),
        yyRuleInfoEntry( 37, 3 ),
        yyRuleInfoEntry( 33, 1 ),
        yyRuleInfoEntry( 35, 1 ),
        yyRuleInfoEntry( 35, 1 ),
        yyRuleInfoEntry( 35, 3 ),
        yyRuleInfoEntry( 38, 1 ),
        yyRuleInfoEntry( 38, 3 ),
        yyRuleInfoEntry( 36, 1 ),
        yyRuleInfoEntry( 34, 1 ),
        ]


    # Action code for each rule follows.
    def action_000(self):
        # alter ::= phrases
        return self.delegate.p_alter_start(
            ps = self.yystack[-1].minor,
            )
    def action_001(self):
        # phrases ::= phrase
        return self.delegate.p_phrases_one(
            p = self.yystack[-1].minor,
            )
    def action_002(self):
        # phrases ::= phrases T_COMMA phrase
        return self.delegate.p_phrases_many(
            ps = self.yystack[-3].minor,
            p = self.yystack[-1].minor,
            )
    def action_003(self):
        # phrase ::=
        return self.delegate.p_phrase_none(
            )
    def action_004(self):
        # phrase ::= K_ENSURE variable_token_opt columns dependency
        return self.delegate.p_phrase_set_var_dependency(
            cols = self.yystack[-2].minor,
            dep = self.yystack[-1].minor,
            )
    def action_005(self):
        # phrase ::= K_ENSURE variable_token_opt columns K_IN view_token K_OF column_name
        return self.delegate.p_phrase_set_var_cluster(
            cols0 = self.yystack[-5].minor,
            col1 = self.yystack[-1].minor,
            )
    def action_006(self):
        # phrase ::= K_ENSURE variable_token_opt columns K_IN K_SINGLETON view_token
        return self.delegate.p_phrase_set_var_cluster_singleton(
            cols = self.yystack[-4].minor,
            )
    def action_007(self):
        # phrase ::= K_SET view_token K_CONCENTRATION K_PARAMETER K_TO concentration
        return self.delegate.p_phrase_set_var_cluster_conc(
            conc = self.yystack[-1].minor,
            )
    def action_008(self):
        # phrase ::= K_ENSURE K_ROW|K_ROWS rows K_IN K_CLUSTER K_OF K_ROW row_index K_WITHIN view_token K_OF column_name
        return self.delegate.p_phrase_set_row_cluster(
            rows0 = self.yystack[-10].minor,
            row1 = self.yystack[-5].minor,
            col = self.yystack[-1].minor,
            )
    def action_009(self):
        # phrase ::= K_ENSURE K_ROW|K_ROWS rows K_IN K_SINGLETON K_CLUSTER K_WITHIN view_token K_OF column_name
        return self.delegate.p_phrase_set_row_cluster_singleton(
            rows0 = self.yystack[-8].minor,
            col = self.yystack[-1].minor,
            )
    def action_010(self):
        # phrase ::= K_SET K_ROW K_CLUSTER K_CONCENTRATION K_PARAMETER K_WITHIN view_token K_OF column_name K_TO concentration
        return self.delegate.p_phrase_set_row_cluster_conc(
            col = self.yystack[-3].minor,
            conc = self.yystack[-1].minor,
            )
    def action_011(self):
        # variable_token_opt ::=
        return None
    def action_012(self):
        # variable_token_opt ::= K_VARIABLE
        return None
    def action_013(self):
        # variable_token_opt ::= K_VARIABLES
        return None
    def action_014(self):
        # view_token ::= K_VIEW
        return None
    def action_015(self):
        # view_token ::= K_CONTEXT
        return None
    def action_016(self):
        # dependency ::= K_INDEPENDENT
        return self.delegate.p_dependency_independent(
            )
    def action_017(self):
        # dependency ::= K_DEPENDENT
        return self.delegate.p_dependency_dependent(
            )
    def action_018(self):
        # columns ::= column_name
        return self.delegate.p_columns_one(
            col = self.yystack[-1].minor,
            )
    def action_019(self):
        # columns ::= T_STAR
        return self.delegate.p_columns_all(
            )
    def action_020(self):
        # columns ::= T_LROUND column_list T_RROUND
        return self.delegate.p_columns_many(
            cols = self.yystack[-2].minor,
            )
    def action_021(self):
        # column_list ::= column_name
        return self.delegate.p_column_list_one(
            col = self.yystack[-1].minor,
            )
    def action_022(self):
        # column_list ::= column_list T_COMMA column_name
        return self.delegate.p_column_list_many(
            cols = self.yystack[-3].minor,
            col = self.yystack[-1].minor,
            )
    def action_023(self):
        # column_name ::= L_NAME
        return self.delegate.p_column_name_n(
            n = self.yystack[-1].minor,
            )
    def action_024(self):
        # rows ::= row_index
        return self.delegate.p_rows_one(
            row = self.yystack[-1].minor,
            )
    def action_025(self):
        # rows ::= T_STAR
        return self.delegate.p_rows_all(
            )
    def action_026(self):
        # rows ::= T_LROUND row_list T_RROUND
        return self.delegate.p_rows_many(
            rows = self.yystack[-2].minor,
            )
    def action_027(self):
        # row_list ::= row_index
        return self.delegate.p_row_list_one(
            row = self.yystack[-1].minor,
            )
    def action_028(self):
        # row_list ::= row_list T_COMMA row_index
        return self.delegate.p_row_list_many(
            rows = self.yystack[-3].minor,
            row = self.yystack[-1].minor,
            )
    def action_029(self):
        # row_index ::= L_NUMBER
        return self.delegate.p_row_index_n(
            n = self.yystack[-1].minor,
            )
    def action_030(self):
        # concentration ::= L_NUMBER
        return self.delegate.p_concentration_c(
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
        action_026,
        action_027,
        action_028,
        action_029,
        action_030,
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


