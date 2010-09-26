(in-package :tottori-nando.internal)

(defgeneric get-op* (db kbuf ksiz))

(defclass db ()
  ((count :initform 0)))

(defun get-op (db key)
  (let ((kbuf (string-to-octets key)))
    (multiple-value-bind (vbuf vsiz) (get-op* db kbuf (length kbuf))
      (when vbuf
        (octets-to-string vbuf :end vsiz)))))

(defun value (db key)
  (get-op db key))

(defgeneric set-op* (db kbuf ksiz vbuf vsiz))

(defun set-op (db key value)
  (let ((kbuf (string-to-octets key))
        (vbuf (string-to-octets value)))
    (set-op* db kbuf (length kbuf) vbuf (length vbuf))))

(defun (setf value) (value db key)
  (set-op db key value))

(defgeneric add-op* (db kbuf ksiz vbuf vsiz))

(defun add-op (db key value)
  (let ((kbuf (string-to-octets key))
        (vbuf (string-to-octets value)))
    (add-op* db kbuf (length kbuf) vbuf (length vbuf))))

(defgeneric replace-op* (db kbuf ksiz vbuf vsiz))

(defun replace-op (db key value)
  (let ((kbuf (string-to-octets key))
        (vbuf (string-to-octets value)))
    (replace-op* db kbuf (length kbuf) vbuf (length vbuf))))

(defgeneric append-op* (db kbuf ksiz kvbuf ksiz))

(defun append-op (db key value)
  (let ((kbuf (string-to-octets key))
        (vbuf (string-to-octets value)))
    (append-op* db kbuf (length kbuf) vbuf (length vbuf))))

(defgeneric inc-op* (db kbuf ksiz delta vsiz))

(defun inc-op (db key delta vsiz)
  (let ((kbuf (string-to-octets key)))
    (inc-op* db kbuf (length kbuf) delta vsiz)))

(defgeneric cas-op* (db kbuf ksiz old-vbuf old-vsiz new-vbuf new-vsiz))

(defun cas-op (db key old-value new-value)
  (let ((kbuf (string-to-octets key))
        (old-vbuf (string-to-octets old-value))
        (new-vbuf (string-to-octets new-value)))
    (cas-op* db
             kbuf (length kbuf)
             old-vbuf (length old-vbuf)
             new-vbuf (length new-vbuf))))

(defgeneric delete-op* (db kbuf ksiz))

(defun delete-op (db key)
  (let ((kbuf (string-to-octets key)))
    (delete-op* db kbuf (length kbuf))))

(defgeneric clear (db))

(defgeneric count-record (db))

(defgeneric cursor (db))

(defclass cursor ()
  ((db :initarg :db :accessor db)))

(defgeneric cursor-accept (cursor visitor &optional writable step))

(defgeneric cursor-key (cursor &optional step))

(defgeneric cursor-value (cursor &optional step))

(defgeneric (setf cursor-value) (value cursor &optional step))

(defgeneric cursor-remove (cursor))

(defgeneric cursor-pair (cursor &optional step))

(defgeneric jump (cursor &optional key))

(defgeneric jump-back (cursor &optional key))

(defgeneric setp-next (cursor))

(defgeneric setp-back (cursor))

