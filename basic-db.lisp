(in-package :tottori-nando)


(defconstant +libver+ 5)
(defconstant +librev+ 2)
(defconstant +formatver+ 5)

(defclass db ()
  ((count :initform 0)))

(defgeneric accept (db kbuf ksiz writable full empty))

(defgeneric get-op* (db kbuf ksiz))

(defun get-op (db key)
  (let ((kbuf (string-to-octets key)))
    (multiple-value-bind (vbuf vsiz) (get-op* db kbuf (length kbuf))
      (when vbuf
        (octets-to-string vbuf :end vsiz)))))

(declaim (inline value))
(defun value (db key)
  (get-op db key))

(defgeneric set-op* (db kbuf ksiz vbuf vsiz))

(defun set-op (db key value)
  (let ((kbuf (string-to-octets key))
        (vbuf (string-to-octets value)))
    (set-op* db kbuf (length kbuf) vbuf (length vbuf))))

(declaim (inline value))
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


(defun full-nop (kbuf ksiz vbuf vsiz)
  (declare (ignore kbuf ksiz vbuf vsiz))
  :nop)

(defun empty-nop (kbuf ksiz)
  (declare (ignore kbuf ksiz))
  :nop)



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


(defconstant +typevoid+   #x00 "void database")
(defconstant +typephash+  #x10 "prototype hash database")
(defconstant +typeptree+  #x11 "prototype tree database")
(defconstant +typecache+  #x20 "cache hash database")
(defconstant +typegrass+  #x21 "cache tree database")
(defconstant +typehash+   #x30 "file hash database")
(defconstant +typetree+   #x31 "file tree database")
(defconstant +typedir+    #x40 "directory hash database")
(defconstant +typeforest+ #x41 "directory tree database")
(defconstant +typemisc+   #x80 "miscellaneous database")

(defclass basic-db (db)
  ())

(defclass basic-db-cursor (cursor)
  ())

(defmethod (setf cursor-value) (valeu (cursor basic-db-cursor) &optional step)
  step)

(defmethod set-op* ((db basic-db) kbuf ksiz vbuf vsiz)
  (accept db kbuf ksiz t
          (lambda (kb ks vb vs)
            (declare (ignore kb ks vb vs))
            (values vbuf vsiz))
          (lambda (kb ks)
            (declare (ignore kb ks))
            (values vbuf vsiz))))

(defmethod get-op* ((db basic-db) kbuf ksiz)
  (let (vbuf vsiz)
    (accept db kbuf ksiz nil
            (lambda (kb ks vb vs)
              (declare (ignore kb ks))
              (setf (values vbuf vsiz) (values vb vs))
              :nop)
            #'empty-nop)
    (values vbuf vsiz)))

(defmethod add-op* ((db basic-db) kbuf ksiz vbuf vsiz)
  (accept db kbuf ksiz t
          #'full-nop
          (lambda (kb ks)
            (declare (ignore kb ks))
            (values vbuf vsiz))))

(defmethod replace-op* ((db basic-db) kbuf ksiz vbuf vsiz)
  (let (ok)
    (accept db kbuf ksiz t
            (lambda (kb ks vb vs)
              (declare (ignore kb ks))
              (setf ok t)
              (values vb vs))
            #'empty-nop)
    ok))

(defmethod append-op* ((db basic-db) kbuf ksiz vbuf vsiz)
  (accept db kbuf ksiz t
          (lambda (kb ks vb vs)
            (declare (ignore kb ks))
            (values (concatenate t vb vbuf) (+ vs vsiz)))
          (lambda (kb ks)
            (declare (ignore kb ks))
            (values vbuf vsiz))))

(defmethod inc-op* ((db basic-db) kbuf ksiz delta vsiz)
  (let (ret)
    (accept db kbuf ksiz t
            (lambda (kb ks vb vs)
              (declare (ignore kb ks))
              (loop repeat vs
                    for i across vb
                    for n = i then (+ (ash n 8) i)
                    finally (let ((vbuf (make-array vsiz :element-type '(unsigned-byte 8))))
                              (setf ret n)
                              (iterate ((x (scan-byte (+ n delta) vsiz))
                                        (i (scan-range)))
                                (setf (aref vbuf i) x))
                              (return (values vbuf vsiz)))))
            (lambda (kb ks)
              (declare (ignore kb ks))
              (let ((vbuf (make-array vsiz :element-type '(unsigned-byte 8))))
                (setf ret 0)
                (iterate ((x (scan-byte delta vsiz))
                          (i (scan-range)))
                  (setf (aref vbuf i) x))
                (values vbuf vsiz))))
    ret))


(defmethod cas-op* ((db basic-db) kbuf ksiz old-vbuf old-vsiz new-vbuf new-vsiz)
  (let (ok)
    (accept db kbuf ksiz t
            (lambda (kb ks vb vs)
              (declare (ignore kb ks))
              (if (and (= vs old-vsiz) (equalp vb old-vbuf))
                  (progn
                    (setf ok t)
                    (values new-vbuf new-vsiz))
                  :nop))
            (lambda (kb ks)
              (declare (ignore kb ks))
              :nop))
    ok))

(defmethod delete-op* ((db basic-db) kbuf ksiz)
  (let (ok)
    (accept db kbuf ksiz t
            (lambda (kb ks vb vs)
              (declare (ignore kb ks vb))
              (setf ok t)
              (values :remove vs))
            #'empty-nop)
    ok))


(defclass proto-db (basic-db)
  ((mlock_ :initform (make-instance 'spin-rw-lock))
   (omode_ :initform 0)
   (recs_ :initform (make-hash-table :test #'equalp))
   (curs_)
   (path_)
   (size_ :initform 0)
   (opaque_)
   (tran_ :initform nil)
   (trlogs_)
   (trsize_)))

(defmethod accept ((db proto-db) kbuf ksiz writable full empty)
  (with-slots (mlock_ recs_ size_) db
    (if writable
        ;; 更新系
        (with-spin-rw-lock (mlock_ t)
          (multiple-value-bind (value found) (gethash kbuf recs_)
            (if found
                ;; 該当あり
                (multiple-value-bind (vbuf vsiz) (funcall full kbuf ksiz value (length value))
                  (case vbuf
                    (:remove
                       ;; 削除
                       (decf size_ (+ ksiz vsiz))
                       (remhash kbuf recs_))
                    (:nop t)
                    (t
                       ;; 置き換え（+nop+ ではない場合）
                       (decf size_ (length value))
                       (incf size_ vsiz)
                       (setf (gethash kbuf recs_) vbuf))))
                ;; 該当なし
                (multiple-value-bind (vbuf vsiz) (funcall empty kbuf ksiz)
                  (case vbuf
                    ((:nop :remove) t)
                    (t
                       (incf size_ (+ ksiz vsiz))
                       (setf (gethash kbuf recs_) vbuf)))))))
        ;; 参照系
        (with-spin-rw-lock (mlock_ nil)
          (aif (gethash kbuf recs_)
               ;; 該当あり
               (funcall full kbuf ksiz it (length it))
               ;; 該当なし
               (funcall empty kbuf ksiz))))))

(defun test-proto-db ()
  (let ((db (make-instance 'proto-db)))
    (set-op db "a" "ABC")
    (assert (equal "ABC" (get-op db "a")))
    (cas-op db "a" "ABC" "xyz")
    (assert (equal "xyz" (get-op db "a")))
    (assert (delete-op db "a"))
    (assert (null (get-op db "a")))
    ;;(set-op* db "inc" 3 #(1) 1)
    ;;(assert (= 1 (print (inc-op db "inc" 2 1))))
    ;;(assert (= 3 (inc-op db "inc" 0 1)))))
    ))
