(defpackage :tottori-nando.skip-list-db
    (:use :cl :tottori-nando :series)
  (:shadowing-import-from :series let let* multiple-value-bind funcall defun))

(series::install :pkg :tottori-nando.skip-list-db)
