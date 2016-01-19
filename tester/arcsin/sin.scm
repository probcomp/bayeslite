; globals
(assume noise (tag (quote global) (quote noise) (beta 1 3)))
; rows
(assume u (mem (lambda (rowid)
    (tag rowid (quote u)
        (uniform_continuous -3.14 3.14)))))
(assume x (mem (lambda (rowid)
    (tag rowid (quote x)
    (normal (sin (u rowid)) noise)))))
; conventions
(assume columns (list u x))
(assume get_cell (lambda (col rowid)
    ((lookup columns col) rowid)))