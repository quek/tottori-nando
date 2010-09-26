(defpackage :tottori-nando.skip-list-memory-db
    (:use :cl :tottori-nando :tottori-nando.internal :series)
  (:shadowing-import-from :series let let* multiple-value-bind funcall defun))

(series::install :pkg :tottori-nando.skip-list-memory-db)
