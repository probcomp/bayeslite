
import sys


def generateGrammar():
    from lemonade.main import generate
    from os.path import join, dirname
    from StringIO import StringIO

    inputFile = join(dirname(__file__), "gram.y")
    outputStream = StringIO()
    generate(inputFile, outputStream)
    return outputStream.getvalue()


# generate and import our grammar
exec generateGrammar() in globals()


#
# the lexer
#

tokenType = {
    '+': PLUS,
    '-': MINUS,
    '/': DIVIDE,
    '*': TIMES,
    }

def tokenize(input):
    import re
    tokenText = re.split("([+-/*])|\s*", input)
    for text in tokenText:
        if text is None:
            continue
        type = tokenType.get(text)
        if type is None:
            type = NUM
            value = float(text)
        else:
            value = None
        yield (type, value)
    return


#
# the delegate
#

class Delegate(object):

    def accept(self):
        return

    def parse_failed(self):
        assert False, "Giving up.  Parser is hopelessly lost..."

    def syntax_error(self, token):
        print >>sys.stderr, "Syntax error!"
        return


    #
    # reduce actions
    #

    def sub(self, a, b):  return a - b
    def add(self, a, b):  return a + b
    def mul(self, a, b):  return a * b
    def div(self, a, b):  return a / b
    def num(self, value): return value

    def print_result(self, result):
        print result
        return


p = Parser(Delegate())
#p.trace(sys.stdout, "# ")

if len(sys.argv) == 2:
    p.parse(tokenize(sys.argv[1]))
else:
    print >>sys.stderr, "usage: %s EXPRESSION" % sys.argv[0]

