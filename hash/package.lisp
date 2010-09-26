(defpackage :tottori-nando.hash
    (:use :cl :tottori-nando :tottori-nando.internal :anaphora :series)
  (:shadowing-import-from :series let let* multiple-value-bind funcall defun)
  (:export #:hash-db))

(series::install :pkg :tottori-nando.hash)
