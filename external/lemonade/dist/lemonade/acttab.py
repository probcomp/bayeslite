'''
This module implements routines use to construct the yy_action[] table.
'''


from struct import *
from ccruft import struct


# The state of the yy_action table under construction is an instance
# of the following structure.

acttab = struct(
    'acttab',
    (
        'nAction',                 # Number of used slots in aAction[]
        'nActionAlloc',            # Slots allocated for aAction[]
        'aAction',                 # The yy_action[] table under construction
        'aLookahead',              # A single new transaction set
        'mnLookahead',             # Minimum aLookahead[].lookahead
        'mnAction',                # Action associated with mnLookahead
        'mxLookahead',             # Maximum aLookahead[].lookahead
        'nLookahead',              # Used slots in aLookahead[]
        'nLookaheadAlloc',         # Slots allocated in aLookahead[]
        )
    )


action = struct(
    'action',
    (
        'lookahead',               # Value of the lookahead token
        'action',                  # Action to take on the given lookahead
        )
    )


def acttab_size(X):
    '''Return the number of entries in the yy_action table.'''
    return X.nAction


def acttab_yyaction(X, N):
    '''The value for the N-th entry in yy_action.'''
    return X.aAction[N].action


def acttab_yylookahead(X,N):
    '''The value for the N-th entry in yy_lookahead.'''
    return X.aAction[N].lookahead


def acttab_free(p):
    return


def acttab_alloc():
    return acttab(0, 0, [], [], 0, 0, 0, 0, 0)


def acttab_action(p, lookahead, _action):
    '''Add a new action to the current transaction set.'''

    if p.nLookahead >= p.nLookaheadAlloc:
        p.nLookaheadAlloc += 25
        p.aLookahead.extend([action(0,0) for i in range(25)])

    if p.nLookahead == 0:
        p.mxLookahead = lookahead
        p.mnLookahead = lookahead
        p.mnAction = _action
    else:
        if p.mxLookahead < lookahead:
            p.mxLookahead = lookahead

        if p.mnLookahead > lookahead:
            p.mnLookahead = lookahead
            p.mnAction = _action

    p.aLookahead[p.nLookahead].lookahead = lookahead
    p.aLookahead[p.nLookahead].action = _action
    p.nLookahead += 1

    return


def acttab_insert(p):
    '''Add the transaction set built up with prior calls to
    acttab_action() into the current action table.  Then reset the
    transaction set back to an empty set in preparation for a new
    round of acttab_action() calls.
    
    Return the offset into the action table of the new transaction.
    '''

    assert p.nLookahead > 0

    # Make sure we have enough space to hold the expanded action table
    # in the worst case.  The worst case occurs if the transaction set
    # must be appended to the current action table.

    n = p.mxLookahead + 1
    if p.nAction + n >= p.nActionAlloc:
        oldAlloc = p.nActionAlloc
        p.nActionAlloc = p.nAction + n + p.nActionAlloc + 20
        p.aAction.extend([action(-1,-1) for i in range(p.nActionAlloc - oldAlloc)])


    # Scan the existing action table looking for an offset where we
    # can insert the current transaction set.  Fall out of the loop
    # when that offset is found.  In the worst case, we fall out of
    # the loop when i reaches p.nAction, which means we append the new
    # transaction set.
    #
    # i is the index in p.aAction[] where p.mnLookahead is inserted.

    i = 0 # in case the range is empty
    for i in range(p.nAction + p.mnLookahead):
        if p.aAction[i].lookahead < 0:
            for j in range(p.nLookahead):
                k = p.aLookahead[j].lookahead - p.mnLookahead + i
                if k < 0:
                    break
                if p.aAction[k].lookahead >= 0:
                    break
            else:
                for j in range(p.nAction):
                    if p.aAction[j].lookahead == j + p.mnLookahead - i:
                        break
                else:
                    break # Fits in empty slots

        elif p.aAction[i].lookahead == p.mnLookahead:
            if p.aAction[i].action != p.mnAction:
                continue
            for j in range(p.nLookahead):
                k = p.aLookahead[j].lookahead - p.mnLookahead + i
                if k < 0 or k >= p.nAction:
                    break
                if p.aLookahead[j].lookahead != p.aAction[k].lookahead:
                    break
                if p.aLookahead[j].action != p.aAction[k].action:
                    break
            else:
                n = 0
                for j in range(p.nAction):
                    if p.aAction[j].lookahead < 0:
                        continue
                    if p.aAction[j].lookahead == j + p.mnLookahead - i:
                        n += 1
                if n == p.nLookahead:
                    break # Same as a prior transaction set

    # Insert transaction set at index i.
    for j in range(p.nLookahead):
        k = p.aLookahead[j].lookahead - p.mnLookahead + i
        p.aAction[k].lookahead = p.aLookahead[j].lookahead
        p.aAction[k].action    = p.aLookahead[j].action
        if k >= p.nAction:
            p.nAction = k + 1
    p.nLookahead = 0
    
    # Return the offset that is added to the lookahead in order to get
    # the index into yy_action of the action.
    return i - p.mnLookahead

