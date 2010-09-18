(defpackage :tottori-nando
  (:use :cl :anaphora :series)
  (:export #:hash-db
           #:db-open
           #:db-close
           #:value))

(series::install :pkg :tottori-nando)

