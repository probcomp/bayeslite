'''
Set manipulation routines for the LEMON parser generator.
'''

size = 0


def SetSize(n):
    '''Set the set size.'''
    global size
    size = n + 1
    return


def SetNew():
    '''Allocate a new set.'''
    return [False] * size


def SetAdd(s, e):
    '''Add a new element to the set.  Return True if the element was
    added and False if it was already there.
    '''
    rv = s[e]
    s[e] = True
    return not rv


def SetUnion(s1, s2):
    '''Add every element of s2 to s1.  Return True if s1 changes.'''
    progress = False
    for i in range(size):
        if not s2[i]:
            continue
        if not s1[i]:
            progress = True
            s1[i] = True
    return progress


def SetFind(X, Y):
    '''True if Y is in set X.'''
    return X[Y]

