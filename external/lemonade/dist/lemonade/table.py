'''
Code for processing tables in the LEMON parser generator.
'''

from struct import *
from ccruft import *


# There is one instance of the following structure for each
# associative array

s_x = struct(
    's_x', (
        'size',   # The number of available slots.
                  # Must be a power of 2 greater than or equal to 1
        'count',  # Number of currently slots filled
        'tbl',    # The data stored here
        'ht',     # Hash table for lookups
        )
    )

# There is one instance of this structure for every data element in an
# associative array

s_xnode = struct(
    's_xnode', (
        'data',   # The data
        'key',    # The key
        'next',   # Next entry with the same hash
        '_from',  # Previous link
        )
    )


#------------------------------------------------------------------------
# generic routines

def new(size):
    '''Create a new associative array.'''
    return s_x(
        size = size,
        count = 0,
        tbl = [s_xnode(None, None, None, None) for i in range(size)],
        ht = [None] * size,
        )


def insert(array, key, data, hash, cmp):
    '''Insert a new record into the array.  Return True if successful.
    Prior data with the same key is NOT overwritten.
    '''

    if array is None:
        return False

    ph = hash(key)
    h = ph & (array.size - 1)
    np = array.ht[h]
    while np:
        if cmp(np.key, key) == 0:
            # An existing entry with the same key is found.
            # Fail because overwrite is not allowed.
            return False
        np = np.next


    if array.count >= array.size:
        # Need to make the hash table bigger

        size = array.size * 2
        count = array.count
        tbl = [s_xnode(None, None, None, None) for i in range(size)]
        ht = [None] * size

        for i in range(count):
            oldnp = array.tbl[i]
            h = hash(oldnp.key) & (size - 1)
            newnp = tbl[i]
            if ht[h]:
                ht[h]._from = newnp.next
            newnp.next = ht[h]
            newnp.key = oldnp.key
            newnp.data = oldnp.data
            newnp._from = ht[h]
            ht[h] = newnp

        array.size = size
        array.count = count
        array.tbl = tbl
        array.ht = ht


    # Insert the new data

    h = ph & (array.size - 1)
    np = array.tbl[array.count]
    array.count += 1
    np.key = key
    np.data = data
    if array.ht[h]:
        array.ht[h]._from = np.next
    np.next = array.ht[h]
    array.ht[h] = np
    np._from = array.ht[h]

    return True


def find(array, key, hash, cmp):
    '''Return the data assigned to the given key.  Return None if no
    such key.'''
    if array is None:
        return None

    h = hash(key) & (array.size - 1)
    np = array.ht[h]

    while np:
        if cmp(np.key, key) == 0:
            break
        np = np.next

    return np.data if np else None


def arrayof(array):
    '''Return a list of all the data in the table.'''

    if array is None:
        return None

    l = []
    for i in range(array.count):
        l.append(array.tbl[i].data)

    return l


#------------------------------------------------------------------------

def strhash(x):
    h = 0
    for c in x:
        h = 0xffffffff & (h*13 + ord(c))
    return h


def Strsafe(y):
    '''Keep strings in a table so that the same string is not in more
    than one place.
    '''

    if y is None:
        return None

    z = Strsafe_find(y)
    if z is None:
        z = y
        Strsafe_insert(z)

    return z


# There is only one instance of the array, which is the following
x1a = None


def Strsafe_init():
    global x1a
    if x1a:
        return
    x1a = new(1024)
    return


def Strsafe_insert(data):
    return insert(x1a, data, data, strhash, strcmp)


def Strsafe_find(key):
    return find(x1a, key, strhash, strcmp)


def Symbol_new(x):
    '''
    Return a the (terminal or nonterminal) symbol "x".  Create a new
    symbol if this is the first time "x" has been seen.
    '''
    
    sp = Symbol_find(x)

    if sp is None:
        sp = symbol(
            name = Strsafe(x),
            type = TERMINAL if x[0].isupper() else NONTERMINAL,
            rule = None,
            fallback = None,
            prec = -1,
            assoc = UNK,
            firstset = None,
            _lambda = False,
            useCnt = 0,
            index = 0,
            nsubsym = 0,
            subsym = None,
            )
        Symbol_insert(sp, sp.name)

    sp.useCnt += 1
    return sp


def Symbolcmpp(a, b):
    '''Compare two symbols for working purposes.'''
    
    # Symbols that begin with upper case letters (terminals or tokens)
    # must sort before symbols that begin with lower case letters
    # (non-terminals).  Other than that, the order does not matter.
    #
    # We find experimentally that leaving the symbols in their
    # original order (the order they appeared in the grammar file)
    # gives the smallest parser tables in SQLite.

    # 2012-06-28 lcs: Additionally, '$' must sort first, and
    # '{default}' must sort last.  Apparently, this is guaranteed by
    # some mysterious property of the associative array implementation
    # in this file.

    # if a.name == '$':
    #     i1 = -1
    # elif a.name == '{default}':
    #     i1 = 20000000
    # else:
    if True:
        i1 = a.index + 10000000*(a.name[0] > 'Z')
    # if b.name == '$':
    #     i2 = -1
    # elif b.name == '{default}':
    #     i2 = 20000000
    # else:
    if True:
        i2 = b.index + 10000000*(b.name[0] > 'Z')
    assert i1 != i2 or a.name == b.name
    return i1 - i2


# There is only one instance of the array, which is the following
x2a = None


def Symbol_init():
    global x2a
    if x2a:
        return
    x2a = new(128)
    return


def Symbol_insert(data, key):
    return insert(x2a, key, data, strhash, strcmp)


def Symbol_find(key):
    return find(x2a, key, strhash, strcmp)


def Symbol_count():
    '''Return the size of the array.'''
    return x2a.count if x2a else 0


def Symbol_arrayof():
    return arrayof(x2a)


def Configcmp(a, b):
    '''Compare two configurations.'''
    x = a.rp.index - b.rp.index
    if x == 0:
        x = a.dot - b.dot
    return x


def statecmp(a, b):
    '''Compare two states.'''

    rc = 0
    while rc == 0 and a and b:
        rc = a.rp.index - b.rp.index
        if rc == 0:
            rc = a.dot - b.dot
        a = a.bp
        b = b.bp

    if rc == 0:
        if a:
            rc = 1
        if b:
            rc = -1

    return rc


def statehash(a):
    '''Hash a state.'''
    h = 0
    while a:
        h = 0xffffffff & (h*571 + a.rp.index*37 + a.dot)
        a = a.bp
    return h


def State_new():
    '''Return a new state structure.'''
    new = state(
        bp = None,
        cfp = None,
        statenum = 0,
        ap = None,
        nTknAct = 0, nNtAct = 0,
        iTknOfst= 0, iNtOfst = 0,
        iDflt = 0,
        )
    return new


# There is only one instance of the array, which is the following
x3a = None


def State_init():
    global x3a
    if x3a:
        return
    x3a = new(128)
    return


def State_insert(data, key):
    return insert(x3a, key, data, statehash, statecmp)


def State_find(key):
    return find(x3a, key, statehash, statecmp)


def State_arrayof():
    return arrayof(x3a)


def confighash(a):
    '''Hash a configuration.'''
    h = 0
    h = 0xffffffff & (h*571 + a.rp.index*37 + a.dot)
    return h


# There is only one instance of the array, which is the following
x4a = None


def Configtable_init():
    global x4a
    if x4a:
        return
    x4a = new(64)
    return


def Configtable_insert(data):
    return insert(x4a, data, data, confighash, Configcmp)


def Configtable_find(key):
    return find(x4a, key, confighash, Configcmp)


def Configtable_clear():
    ''' Remove all data from the table.'''

    if x4a is None or x4a.count == 0:
        return

    for i in range(x4a.size):
        x4a.ht[i] = None

    x4a.count = 0

    return

