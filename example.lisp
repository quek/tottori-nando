(defpackage :tottori-nando-example
    (:use :cl :tottori-nando :series)
  (:shadowing-import-from :series #:defun #:let #:let* #:multiple-value-bind #:funcall))

(in-package :tottori-nando-example)

(series::install)

(defmacro with-db ((var path) &body body)
  `(let ((,var (make-instance 'hash-db)))
     (db-open ,var ,path)
     (unwind-protect
          (progn ,@body)
       (db-close db))))

(time
 (with-db (db "/tmp/tottori-nando-example")
   (let ((data (loop for i from 1 to 10000
                     collect (list (format nil "key~a" i)
                                   (format nil "value~a" i)))))
     (loop for (k v) in data
           do (setf (value db k) v)
           do (assert (equal v (value db k)))))))

