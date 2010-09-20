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

(defun example1 ()
  (time
   (with-db (db "/tmp/tottori-nando-example")
     (loop for i from 1 to 100000
           for k = (format nil "key~a" i)
           for v = (format nil "value~a" i)
           if (zerop (mod i 10000))
             do (format t "~&~d" i)
           do (setf (value db k) v)
           do (assert (equal v (value db k)))))))


(defun example2 ()
  (time
   (with-db (db "/tmp/tottori-nando-example")
     (loop for i from 1 to 10000
           for k = (format nil "key~a" i)
           for v = (format nil "value~a" i)
           do (setf (value db k) v))
     (loop for i from 1 to 10000
           for k = (format nil "key~a" i)
           for v = (format nil "new-value~a" i)
           do (setf (value db k) v)
           do (assert (equal v (value db k)))))))
