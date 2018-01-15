# -*- Python -*-

# Driver template for the LEMON parser generator.
# The author disclaims copyright to this source code.


# First off, define the token values.  These constants (all generated
# automatically by the parser generator) specify the various kinds of
# tokens (terminals) that the parser understands.
#
# Each symbol here is a terminal symbol in the grammar.

T_SEMI                         =  1
T_COMMA                        =  2
K_SET                          =  3
K_CATEGORY                     =  4
K_MODEL                        =  5
K_FOR                          =  6
K_USING                        =  7
K_TO                           =  8
K_OVERRIDE                     =  9
K_SUBSAMPLE                    = 10
L_NUMBER                       = 11
K_LATENT                       = 12
L_NAME                         = 13
K_GENERATIVE                   = 14
K_GIVEN                        = 15
K_EXPOSE                       = 16
K_AND                          = 17
T_LROUND                       = 18
T_RROUND                       = 19
T_EQ                           = 20

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
    YYNSTATE = 58
    YYNRULE = 32
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

    YY_ACTTAB_COUNT = 85
    yy_action = [
           58,   19,   19,   35,   91,   21,   36,   35,   17,   16, #     0
           25,   11,    9,   16,   25,   10,    9,   59,    1,    1, #    10
           52,   27,   51,   23,   50,   54,   34,   40,    3,   40, #    20
           33,    7,   20,   26,   29,   76,   42,   28,   56,   57, #    30
           22,   30,   15,    5,    4,   13,   10,   24,    6,   38, #    40
           18,   14,   48,   49,   53,   55,   39,   47,   32,   46, #    50
           31,   44,   92,   92,    2,   92,   45,   12,   43,   92, #    60
           92,   92,   92,   41,   92,    8,   92,   92,   92,   92, #    70
           92,   92,   92,   92,   37, #    80
        ]
    yy_lookahead = [
            0,    7,    8,    3,   22,   23,   24,    3,    2,    9, #     0
           10,   26,   12,    9,   10,    2,   12,    0,    1,    2, #    10
           11,   36,   13,   37,   38,   19,    4,   26,   15,   26, #    20
            5,   30,    6,   30,   32,   16,   17,   35,   24,   25, #    30
           20,    5,    7,    6,   16,    2,    2,   26,   18,   11, #    40
           27,   33,   13,   13,   38,   28,   14,   28,   13,   13, #    50
           29,   13,   39,   39,   31,   39,   34,   26,   34,   39, #    60
           39,   39,   39,   26,   39,   26,   39,   39,   39,   39, #    70
           39,   39,   39,   39,   34, #    80
        ]
    YY_SHIFT_USE_DFLT = -7
    YY_SHIFT_COUNT = 35
    YY_SHIFT_MIN = -6
    YY_SHIFT_MAX = 48
    yy_shift_ofst = [
            0,    4,   19,   39,   39,   39,   45,   13,   48,   39, #     0
           39,   48,   48,   39,   30,   46,   42,   45,   30,   40, #    10
           39,   17,    9,    6,   -6,   38,   44,   43,   28,   35, #    20
           37,   36,   20,   26,   25,   22, #    30
        ]
    YY_REDUCE_USE_DFLT = -19
    YY_REDUCE_COUNT = 20
    YY_REDUCE_MIN = -18
    YY_REDUCE_MAX = 50
    yy_reduce_ofst = [
          -18,   14,    2,    3,  -15,    1,  -14,   33,   50,   49, #     0
           47,   34,   32,   41,   29,   18,   31,   16,   27,   23, #    10
           21, #    20
        ]
    yy_default = [
           90,   62,   74,   90,   90,   90,   90,   72,   90,   90, #     0
           90,   90,   90,   90,   84,   90,   70,   90,   84,   90, #    10
           90,   90,   90,   90,   90,   90,   73,   75,   90,   90, #    20
           90,   90,   90,   90,   90,   90,   60,   67,   66,   71, #    30
           80,   81,   77,   78,   83,   79,   69,   65,   82,   68, #    40
           86,   89,   88,   87,   85,   64,   63,   61, #    50
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
        "$",                   "T_SEMI",              "T_COMMA",             "K_SET",       
        "K_CATEGORY",          "K_MODEL",             "K_FOR",               "K_USING",     
        "K_TO",                "K_OVERRIDE",          "K_SUBSAMPLE",         "L_NUMBER",    
        "K_LATENT",            "L_NAME",              "K_GENERATIVE",        "K_GIVEN",     
        "K_EXPOSE",            "K_AND",               "T_LROUND",            "T_RROUND",    
        "T_EQ",                "error",               "cgpm",                "schema",      
        "clause",              "clause_opt",          "var",                 "dist",        
        "param_opt",           "generative_opt",        "vars",                "given_opt",   
        "exposing_opt",        "foreign",             "stattype",            "and_opt",     
        "exposed",             "params",              "param",       
        ]

    # For tracing reduce actions, the names of all rules are required.
    yyRuleName = [
        "cgpm ::=", #   0
        "cgpm ::= schema", #   1
        "schema ::= clause", #   2
        "schema ::= schema T_SEMI|T_COMMA clause_opt", #   3
        "clause_opt ::=", #   4
        "clause_opt ::= clause", #   5
        "clause ::= K_SET K_CATEGORY K_MODEL K_FOR var K_USING|K_TO dist param_opt", #   6
        "clause ::= K_OVERRIDE generative_opt K_MODEL K_FOR vars given_opt exposing_opt K_USING foreign param_opt", #   7
        "clause ::= K_SUBSAMPLE L_NUMBER", #   8
        "clause ::= K_LATENT var stattype", #   9
        "dist ::= L_NAME", #  10
        "foreign ::= L_NAME", #  11
        "generative_opt ::=", #  12
        "generative_opt ::= K_GENERATIVE", #  13
        "given_opt ::=", #  14
        "given_opt ::= K_GIVEN vars", #  15
        "exposing_opt ::=", #  16
        "exposing_opt ::= and_opt K_EXPOSE exposed", #  17
        "and_opt ::=", #  18
        "and_opt ::= K_AND", #  19
        "exposed ::= var stattype", #  20
        "exposed ::= exposed T_COMMA var stattype", #  21
        "vars ::= var", #  22
        "vars ::= vars T_COMMA var", #  23
        "var ::= L_NAME", #  24
        "stattype ::= L_NAME", #  25
        "param_opt ::=", #  26
        "param_opt ::= T_LROUND params T_RROUND", #  27
        "params ::= param", #  28
        "params ::= params T_COMMA param", #  29
        "param ::= L_NAME T_EQ L_NUMBER", #  30
        "param ::= L_NAME T_EQ L_NAME", #  31
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
        yyRuleInfoEntry( 22, 0 ),
        yyRuleInfoEntry( 22, 1 ),
        yyRuleInfoEntry( 23, 1 ),
        yyRuleInfoEntry( 23, 3 ),
        yyRuleInfoEntry( 25, 0 ),
        yyRuleInfoEntry( 25, 1 ),
        yyRuleInfoEntry( 24, 8 ),
        yyRuleInfoEntry( 24, 10 ),
        yyRuleInfoEntry( 24, 2 ),
        yyRuleInfoEntry( 24, 3 ),
        yyRuleInfoEntry( 27, 1 ),
        yyRuleInfoEntry( 33, 1 ),
        yyRuleInfoEntry( 29, 0 ),
        yyRuleInfoEntry( 29, 1 ),
        yyRuleInfoEntry( 31, 0 ),
        yyRuleInfoEntry( 31, 2 ),
        yyRuleInfoEntry( 32, 0 ),
        yyRuleInfoEntry( 32, 3 ),
        yyRuleInfoEntry( 35, 0 ),
        yyRuleInfoEntry( 35, 1 ),
        yyRuleInfoEntry( 36, 2 ),
        yyRuleInfoEntry( 36, 4 ),
        yyRuleInfoEntry( 30, 1 ),
        yyRuleInfoEntry( 30, 3 ),
        yyRuleInfoEntry( 26, 1 ),
        yyRuleInfoEntry( 34, 1 ),
        yyRuleInfoEntry( 28, 0 ),
        yyRuleInfoEntry( 28, 3 ),
        yyRuleInfoEntry( 37, 1 ),
        yyRuleInfoEntry( 37, 3 ),
        yyRuleInfoEntry( 38, 3 ),
        yyRuleInfoEntry( 38, 3 ),
        ]


    # Action code for each rule follows.
    def action_000(self):
        # cgpm ::=
        return self.delegate.p_cgpm_empty(
            )
    def action_001(self):
        # cgpm ::= schema
        return self.delegate.p_cgpm_schema(
            s = self.yystack[-1].minor,
            )
    def action_002(self):
        # schema ::= clause
        return self.delegate.p_schema_one(
            c = self.yystack[-1].minor,
            )
    def action_003(self):
        # schema ::= schema T_SEMI|T_COMMA clause_opt
        return self.delegate.p_schema_some(
            s = self.yystack[-3].minor,
            c = self.yystack[-1].minor,
            )
    def action_004(self):
        # clause_opt ::=
        return self.delegate.p_clause_opt_none(
            )
    def action_005(self):
        # clause_opt ::= clause
        return self.delegate.p_clause_opt_some(
            c = self.yystack[-1].minor,
            )
    def action_006(self):
        # clause ::= K_SET K_CATEGORY K_MODEL K_FOR var K_USING|K_TO dist param_opt
        return self.delegate.p_clause_basic(
            var = self.yystack[-4].minor,
            dist = self.yystack[-2].minor,
            params = self.yystack[-1].minor,
            )
    def action_007(self):
        # clause ::= K_OVERRIDE generative_opt K_MODEL K_FOR vars given_opt exposing_opt K_USING foreign param_opt
        return self.delegate.p_clause_foreign(
            outputs = self.yystack[-6].minor,
            inputs = self.yystack[-5].minor,
            exposed = self.yystack[-4].minor,
            name = self.yystack[-2].minor,
            params = self.yystack[-1].minor,
            )
    def action_008(self):
        # clause ::= K_SUBSAMPLE L_NUMBER
        return self.delegate.p_clause_subsamp(
            n = self.yystack[-1].minor,
            )
    def action_009(self):
        # clause ::= K_LATENT var stattype
        return self.delegate.p_clause_latent(
            var = self.yystack[-2].minor,
            st = self.yystack[-1].minor,
            )
    def action_010(self):
        # dist ::= L_NAME
        return self.delegate.p_dist_name(
            dist = self.yystack[-1].minor,
            )
    def action_011(self):
        # foreign ::= L_NAME
        return self.delegate.p_foreign_name(
            foreign = self.yystack[-1].minor,
            )
    def action_012(self):
        # generative_opt ::=
        return None
    def action_013(self):
        # generative_opt ::= K_GENERATIVE
        return None
    def action_014(self):
        # given_opt ::=
        return self.delegate.p_given_opt_none(
            )
    def action_015(self):
        # given_opt ::= K_GIVEN vars
        return self.delegate.p_given_opt_some(
            vars = self.yystack[-1].minor,
            )
    def action_016(self):
        # exposing_opt ::=
        return self.delegate.p_exposing_opt_none(
            )
    def action_017(self):
        # exposing_opt ::= and_opt K_EXPOSE exposed
        return self.delegate.p_exposing_opt_one(
            exp = self.yystack[-1].minor,
            )
    def action_018(self):
        # and_opt ::=
        return self.delegate.p_and_opt_none(
            )
    def action_019(self):
        # and_opt ::= K_AND
        return self.delegate.p_and_opt_one(
            )
    def action_020(self):
        # exposed ::= var stattype
        return self.delegate.p_exposed_one(
            v = self.yystack[-2].minor,
            s = self.yystack[-1].minor,
            )
    def action_021(self):
        # exposed ::= exposed T_COMMA var stattype
        return self.delegate.p_exposed_many(
            exp = self.yystack[-4].minor,
            v = self.yystack[-2].minor,
            s = self.yystack[-1].minor,
            )
    def action_022(self):
        # vars ::= var
        return self.delegate.p_vars_one(
            var = self.yystack[-1].minor,
            )
    def action_023(self):
        # vars ::= vars T_COMMA var
        return self.delegate.p_vars_many(
            vars = self.yystack[-3].minor,
            var = self.yystack[-1].minor,
            )
    def action_024(self):
        # var ::= L_NAME
        return self.delegate.p_var_name(
            var = self.yystack[-1].minor,
            )
    def action_025(self):
        # stattype ::= L_NAME
        return self.delegate.p_stattype_s(
            st = self.yystack[-1].minor,
            )
    def action_026(self):
        # param_opt ::=
        return self.delegate.p_param_opt_none(
            )
    def action_027(self):
        # param_opt ::= T_LROUND params T_RROUND
        return self.delegate.p_param_opt_some(
            ps = self.yystack[-2].minor,
            )
    def action_028(self):
        # params ::= param
        return self.delegate.p_params_one(
            param = self.yystack[-1].minor,
            )
    def action_029(self):
        # params ::= params T_COMMA param
        return self.delegate.p_params_many(
            params = self.yystack[-3].minor,
            param = self.yystack[-1].minor,
            )
    def action_030(self):
        # param ::= L_NAME T_EQ L_NUMBER
        return self.delegate.p_param_num(
            p = self.yystack[-3].minor,
            num = self.yystack[-1].minor,
            )
    def action_031(self):
        # param ::= L_NAME T_EQ L_NAME
        return self.delegate.p_param_nam(
            p = self.yystack[-3].minor,
            nam = self.yystack[-1].minor,
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
        action_031,
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


