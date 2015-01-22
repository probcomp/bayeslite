'''
Routines processing parser actions in the LEMON parser generator.
'''

from struct import *


def actioncmp(ap1, ap2):
    '''Compare two actions for sorting purposes.  Return negative,
    zero, or positive if the first action is less than, equal to, or
    greater than the first.
    '''
    rc = ap1.sp.index - ap2.sp.index
    if rc == 0:
        rc = ap1.type - ap2.type
    if rc == 0 and ap1.type == REDUCE:
        rc = ap1.x.rp.index - ap2.x.rp.index
    assert rc != 0 or ap1 == ap2
    return rc


def Action_sort(ap):
    '''Sort parser actions.'''
    from msort import msort
    ap = msort(ap, 'next', actioncmp)
    return ap


def Action_add(app, type, sp, arg):
    new = action(
        next = app,
        type = type,
        sp = sp,
        collide = None,
        stp = None,
        rp = None,
        )
    app = new
    if type == SHIFT:
        new.x.stp = arg
    else:
        new.x.rp = arg
    return app

