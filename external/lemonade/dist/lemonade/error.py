'''
Code for printing error message.
'''


def findbreak(msg, min, max):
    '''Find a good place to break "msg" so that its length is at least
    "min" but no more than "max".  Make the point as close to max as
    possible.
    '''
    
    spot = min
    for i in range(min, max+1):
        if i >= len(msg):
            spot = i
            break
        c = msg[i]
        if c == '\t':
            msg[i] = ' '
        if c == '\n':
            msg[i] = ' '
            spot = i
            break
        if c == '-' and i < max - 1:
            spot = i + 1
        if c == ' ':
            spot = i
    return spot


# The error message is split across multiple lines if necessary.  The
# splits occur at a space, if there is a space available near the end
# of the line.

LINEWIDTH = 79    # Max width of any output line
PREFIXLIMIT = 30  # Max width of the prefix on each line

def ErrorMsg(filename, lineno, format, *args):
    from ccruft import fprintf
    from sys import stdout

    # Prepare a prefix to be prepended to every output line
    if lineno > 0:
        prefix = "%.*s:%d: " % (PREFIXLIMIT - 10, filename, lineno)
    else:
        prefix = "%.*s: " % (PREFIXLIMIT - 10, filename)

    # Generate the error message
    prefixsize = len(prefix)
    availablewidth = LINEWIDTH - prefixsize
    errmsg = format % args

    # Remove trailing '\n's from the error message
    while errmsg[-1] == '\n':
        errmsg = errmsg[:-1]

    # Print the error message
    base = 0
    while base < len(errmsg):
        end = restart = findbreak(errmsg[base:], 0, availablewidth)
        restart += base
        while restart < len(errmsg) and errmsg[restart] == ' ':
            restart += 1
        fprintf(stdout, "%s%.*s\n", prefix, end, errmsg[base:])
        base = restart

    return

