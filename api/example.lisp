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
     (loop for i from 1 to 10001
           for k = (format nil "key~a" i)
           for v = (format nil "value~a" i)
           do (setf (value db k) v))
     (loop for i from 1 to 10001
           for k = (format nil "key~a" i)
           for v = (format nil "new-value~a" i)
           do (setf (value db k) v)
           do (assert (equal v (value db k)))))))



(defun example3 ()
  (time
   (let* ((n 100000)
          (db (make-instance 'skip-list-db)))
     (db-open db "/tmp/skip-list-example.db")
     (unwind-protect
          (let ((ns (loop for i from 1 to n
                          for x = (random n)
                          for k = (format nil "key~a" x)
                          for v = (format nil "value~a" x)
                          if (zerop (mod i 10000))
                            do (format t "~&~d" i)
                          do (setf (value db k) v)
                          collect x)))
            (loop for i from 1
                  for x in ns
                  for k = (format nil "key~a" x)
                  for v = (format nil "value~a" x)
                  if (zerop (mod i 10000))
                    do (format t "~&~d" i)
                  do (assert (equal v (value db k)))))
       (db-close db)))))
;; Evaluation took:
;;   8.140 seconds of real time
;;   8.060504 seconds of total run time (7.980499 user, 0.080005 system)
;;   [ Run times consist of 0.172 seconds GC time, and 7.889 seconds non-GC time. ]
;;   99.03% CPU
;;   7 forms interpreted
;;   14,615,417,781 processor cycles
;;   834,702,560 bytes consed
