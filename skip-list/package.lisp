(defpackage :tottori-nando.skip-list-db
    (:use :cl :tottori-nando :tottori-nando.internal :series)
  (:shadowing-import-from :series let let* multiple-value-bind funcall defun)
  (:export #:make-skip-list-db))

(series::install :pkg :tottori-nando.skip-list-db)
