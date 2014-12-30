

def iterlinks(head, next='next'):
    link = head
    while link:
        yield link
        link = getattr(link, next)
    return


def fprintf(stream, format, *args):
    output = format % args
    stream.write(output)
    return len(output)


def printf(format, *args):
    from sys import stdout
    return fprintf(stdout, format, *args)


def fputc(c, stream):
    stream.write(c)
    return c


def strcmp(a,b):
    # no 'cmp' in Python 3
    return (a > b) - (a < b)


#------------------------------------------------------------------------
# struct

# Derived from the named tuples Python recipe:
#   http://code.activestate.com/recipes/500261/

from keyword import iskeyword as _iskeyword
import sys as _sys

def struct(typename, field_names, verbose=False):
    """Returns a new class with named fields.

    >>> Point = struct('Point', 'x y')
    >>> Point.__doc__                   # docstring for the new class
    'Point(x, y)'
    >>> p = Point(11, y=22)             # instantiate with positional args or keywords
    >>> p.x + p.y                       # fields accessable by name
    33
    >>> d = p._asdict()                 # convert to a dictionary
    >>> d['x']
    11
    >>> Point(**d)                      # convert from a dictionary
    Point(x=11, y=22)
    >>> p._replace(x=100)               # _replace() is like str.replace() but targets named fields
    Point(x=100, y=22)

    """

    # Parse and validate the field names.  Validation serves two purposes,
    # generating informative error messages and preventing template injection attacks.
    if isinstance(field_names, basestring):
        field_names = field_names.replace(',', ' ').split() # names separated by whitespace and/or commas
    field_names = tuple(map(str, field_names))
    for name in (typename,) + field_names:
        if not min(c.isalnum() or c=='_' for c in name):
            raise ValueError('Type names and field names can only contain alphanumeric characters and underscores: %r' % name)
        if _iskeyword(name):
            raise ValueError('Type names and field names cannot be a keyword: %r' % name)
        if name[0].isdigit():
            raise ValueError('Type names and field names cannot start with a number: %r' % name)
    seen_names = set()
    for name in field_names:
        if name in seen_names:
            raise ValueError('Encountered duplicate field name: %r' % name)
        seen_names.add(name)

    # Create and fill-in the class template
    argtxt = repr(field_names).replace("'", "")[1:-1]   # tuple repr without parens or quotes
    reprtxt = ', '.join('%s=%%r' % name for name in field_names)
    valuestxt = ', '.join('self.%s' % name for name in field_names)
    othervaluestxt = ', '.join('other.%s' % name for name in field_names)
    template = '''class %(typename)s(object):
        '%(typename)s(%(argtxt)s)' \n
        __slots__ = %(field_names)r \n
        _fields = %(field_names)r \n
        def __init__(self, %(argtxt)s):\n'''
    for name in field_names:
        template += '''            self.%s = %s\n''' % (name, name)
    template += '''            return \n
        def __repr__(self):
            return '%(typename)s(%(reprtxt)s)' %% (%(valuestxt)s) \n
        def __cmp__(self, other):
            if not isinstance(other, self.__class__):
                return -1
            return cmp((%(valuestxt)s), (%(othervaluestxt)s)) \n
        def _asdict(self):
            'Return a new dict which maps field names to their values'
            d = {}
            for field in self._fields:
                d[field] = getattr(self, field)
            return d \n
        def _replace(_self, **kwds):
            'Return a new %(typename)s object replacing specified fields with new values'
            from copy import copy
            result = copy(_self)
            for key, value in kwds.items():
                setattr(result, key, value)
            return result \n\n'''
    template = template % locals()
    if verbose:
        print template

    # Execute the template string in a temporary namespace
    namespace = dict(__name__='struct_%s' % typename)
    try:
        exec template in namespace
    except SyntaxError, e:
        raise SyntaxError(e.message + ':\n' + template)
    result = namespace[typename]

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the struct is created.  Bypass this step in enviroments where
    # sys._getframe is not defined (Jython for example) or sys._getframe is not
    # defined for arguments greater than 0 (IronPython).
    try:
        result.__module__ = _sys._getframe(1).f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass

    return result






if __name__ == '__main__':
    # verify that instances can be pickled
    from cPickle import loads, dumps
    Point = struct('Point', 'x, y', True)
    p = Point(x=10, y=20)
    assert p == loads(dumps(p, -1))

    # test and demonstrate ability to override methods
    class Point(struct('Point', 'x y')):
        @property
        def hypot(self):
            return (self.x ** 2 + self.y ** 2) ** 0.5
        def __str__(self):
            return 'Point: x=%6.3f y=%6.3f hypot=%6.3f' % (self.x, self.y, self.hypot)

    for p in Point(3,4), Point(14,5), Point(9./7,6):
        print p

    print Point(11, 22)._replace(x=100)

    import doctest
    TestResults = struct('TestResults', 'failed attempted')
    print TestResults(*doctest.testmod())
