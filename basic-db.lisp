(in-package :tottori-nando)


(defconstant +libver+ 5)
(defconstant +librev+ 2)
(defconstant +formatver+ 5)

(defclass db ()
  ((count :initform 0)))

(defgeneric accept (db kbuf ksiz writable full empty))

(defgeneric get-value (db kbuf ksiz))

(defgeneric value (db key)
  (:method (db key)
    (let ((kbuf (sb-ext:string-to-octets key :external-format :utf-8)))
      (multiple-value-bind (vbuf vsiz) (get-value db kbuf (length kbuf))
        (when vbuf
          (sb-ext:octets-to-string vbuf :end vsiz :external-format :utf-8))))))

(defgeneric set-value (db kbuf ksiz vbuf vsiz))

(defgeneric (setf value) (value db key)
  (:method (value db key)
    (let ((kbuf (sb-ext:string-to-octets key :external-format :utf-8))
          (vbuf (sb-ext:string-to-octets value :external-format :utf-8)))
      (set-value db kbuf (length kbuf) vbuf (length vbuf)))))

(defgeneric add-value (db kbuf ksiz vbuf vsiz))

(defgeneric replace-value (db kbuf ksiz vbuf vsiz))

(defgeneric append-value (db kbuf ksiz kvbuf ksiz))

(defgeneric increment (db kbuf ksiz delta))

(defgeneric cas (db kbuf ksiz old-vbuf old-vsiz new-vbuf new-vsiz))

(defgeneric remove-value (db kbuf ksiz))

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

(defmethod set-value ((db basic-db) kbuf ksiz vbuf vsiz)
  (accept db kbuf ksiz t
          (lambda (kb ks vb vs)
            (declare (ignore kb ks vb vs))
            (values vbuf vsiz))
          (lambda (kb ks)
            (declare (ignore kb ks))
            (values vbuf vsiz))))

(defmethod get-value ((db basic-db) kbuf ksiz)
  (let (vbuf vsiz)
    (accept db kbuf ksiz nil
            (lambda (kb ks vb vs)
              (declare (ignore kb ks))
              (setf (values vbuf vsiz) (values vb vs))
              :nop)
            #'empty-nop)
    (values vbuf vsiz)))

(defmethod add-value ((db basic-db) kbuf ksiz vbuf vsiz)
  (accept db kbuf ksiz t
          #'full-nop
          (lambda (kb ks)
            (declare (ignore kb ks))
            (values vbuf vsiz))))

(defmethod replace-value ((db basic-db) kbuf ksiz vbuf vsiz)
  (let (ok)
    (accept db kbuf ksiz t
            (lambda (kb ks vb vs)
              (declare (ignore kb ks))
              (setf ok t)
              (values vb vs))
            #'empty-nop)
    ok))

(defmethod append-value ((db basic-db) kbuf ksiz vbuf vsiz)
  (accept db kbuf ksiz t
          (lambda (kb ks vb vs)
            (declare (ignore kb ks))
            (values (concatenate t vb vbuf) (+ vs vsiz)))
          (lambda (kb ks)
            (declare (ignore kb ks))
            (values vbuf vsiz))))

;;(defmethod increment ((db basic-db) kbuf ksiz delta)
;;  (accept db kbuf ksiz t
;;            (lambda (kb ks vb vs)
;;              (declare (ignore kb ks))
;;              (+ v delta))
;;            (lambda (k)
;;              (declare (ignore k))
;;              delta))))
;;
;;(defmethod cas ((db basic-db) key old-value new-value)
;;  (let ((kbuf (sb-ext:string-to-octets key :external-format :utf-8)))
;;    (accept db kbuf (length kbuf) t
;;            (lambda (k v)
;;              (declare (ignore k))
;;              (if (equal v old-value)
;;                  new-value
;;                  :nop))
;;            (lambda (k)
;;              (declare (ignore k))
;;              (when (null old-value)
;;                new-value)))))

(defmethod remove-value ((db basic-db) kbuf ksiz)
  (let (ok)
    (accept db kbuf ksiz t
            (lambda (kb ks vb vs)
              (declare (ignore kb ks vb vs))
              (setf ok t)
              :remove)
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
    (set-value db "a" 1 "ABC" 3)
    (assert (equal "ABC" (get-value db "a" 1)))
    ;;(cas db "a" "ABC" "xyz")
    ;;(assert (equal "xyz" (get-value db "a" 1)))
    (assert (remove-value db "a" 1))
    (assert (null (get-value db "a" 1)))))
