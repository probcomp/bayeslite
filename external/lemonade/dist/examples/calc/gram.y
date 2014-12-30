
%left PLUS MINUS.
%left DIVIDE TIMES.

program(print_result) ::= expr(result).

expr(sub) ::= expr(a) MINUS  expr(b).
expr(add) ::= expr(a) PLUS   expr(b).
expr(mul) ::= expr(a) TIMES  expr(b).
expr(div) ::= expr(a) DIVIDE expr(b).

expr(num) ::= NUM(value).

