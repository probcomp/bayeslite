; constants.
(assume mu (list 10 -10 0))
(assume var (list 1 2 1))
; global latents.
(assume n_cluster 3)
(assume r
    (tag 'glat 'all
        (symmetric_dirichlet .5 n_cluster)))
; row latents.
(assume k (mem (lambda (rowid)
    (tag 'rlat rowid
        (categorical r)))))
; columns
(assume x1
    (mem (lambda (rowid)
        (tag 'rlat rowid
            (normal (lookup mu (k rowid)) (lookup var (k rowid)))))))
(assume x2
    (mem (lambda (rowid)
        (tag 'rlat rowid (poisson (add 1 (k 1)))))))
; convenience
(assume all_cols (list x1 x2))
(assume get_cell
    (lambda (col rowid)
        ((lookup all_cols col) rowid)))
