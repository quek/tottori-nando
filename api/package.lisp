(defpackage :tottori-nando
  (:use :cl :tottori-nando.internal :anaphora :series)
  (:shadowing-import-from :series let let* multiple-value-bind funcall defun)
  (:export #:db
           #:hash-db
           #:db-open
           #:db-close
           #:value
           #:get-op
           #:get-op*
           #:set-op
           #:set-op*
           #:add-op
           #:add-op*
           #:replace-op
           #:replace-op*
           #:append-op
           #:append-op*
           #:inc-op
           #:inc-op*
           #:cas-op
           #:cas-op*
           #:delete-op
           #:delete-op*

           #:make-skip-list-db))

(series::install :pkg :tottori-nando)

