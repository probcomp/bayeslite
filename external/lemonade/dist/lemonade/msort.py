#
# A generic merge-sort program.
#
# USAGE:
# Let "ptr" be a pointer to some structure which is at the head of
# a null-terminated list.  Then to sort the list call:
#
#     ptr = msort(ptr,&(ptr->next),cmpfnc);
#
# In the above, "cmpfnc" is a pointer to a function which compares
# two instances of the structure and returns an integer, as in
# strcmp.  The second argument is a pointer to the pointer to the
# second element of the linked list.  This address is used to compute
# the offset to the "next" field within the structure.  The offset to
# the "next" field must be constant for all structures in the list.
#
# The function returns a new pointer which is the head of the list
# after sorting.
#
# ALGORITHM:
# Merge-sort.
#


#
# Inputs:
#   a:       A sorted, null-terminated linked list.  (May be null).
#   b:       A sorted, null-terminated linked list.  (May be null).
#   cmp:     A pointer to the comparison function.
#   next:    Attribute name of "next" field.
#
# Return Value:
#   A pointer to the head of a sorted list containing the elements
#   of both a and b.
#
# Side effects:
#   The "next" pointers for elements in the lists a and b are
#   changed.
#
def merge(a, b, cmp, next):
    if a is None:
        head = b
    elif b is None:
        head = a
    else:
        if cmp(a, b) <= 0:
            ptr = a
            a = getattr(a, next)
        else:
            ptr = b
            b = getattr(b, next)

        head = ptr
        while a and b:
            if cmp(a, b) <= 0:
                setattr(ptr, next, a)
                ptr = a
                a = getattr(a, next)
            else:
                setattr(ptr, next, b)
                ptr = b
                b = getattr(b, next)

        if a:
            setattr(ptr, next, a)
        else:
            setattr(ptr, next, b)

    return head


#
# Inputs:
#   list:      Pointer to a singly-linked list of structures.
#   next:      Attribute name of "next" field.
#   cmp:       A comparison function.
#
# Return Value:
#   A pointer to the head of a sorted list containing the elements
#   orginally in list.
#
# Side effects:
#   The "next" pointers for elements in list are changed.
#

LISTSIZE = 30

def msort(list, next, cmp):
    set = [None] * LISTSIZE

    while list:
        ep = list
        list = getattr(list, next)
        setattr(ep, next, None)
        i = 0
        while i < LISTSIZE - 1 and set[i]:
            ep = merge(ep, set[i], cmp, next)
            set[i] = None
            i += 1
        set[i] = ep

    ep = None
    for i in range(LISTSIZE):
        if set[i]:
            ep = merge(set[i], ep, cmp, next)

    return ep

