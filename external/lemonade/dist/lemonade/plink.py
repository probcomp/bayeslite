'''
Routines processing configuration follow-set propagation links in the
LEMON parser generator.
'''


def Plink_add(plpp, cfp):
    '''Add a plink to a plink list.'''
    from struct import plink
    new = plink(
        next = plpp,
        cfp = cfp
        )
    return new


def Plink_copy(to, _from):
    '''Transfer every plink on the list "from" to the list "to".'''
    while _from:
        nextpl = _from.next
        _from.next = to
        to = _from
        _from = nextpl
    return to

