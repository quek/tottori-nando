(in-package :tottori-nando.skip-list-db)

(deftype octes () '(simple-array (unsigned-byte 8) (*)))

(defclass head ()
  ((max-level :initarg :max-level :initform 3)
   (record-count :initform 0)))

(defclass record ()
  ((key-start :initarg :key-start :initform 0)
   (key-end :initarg :key-end :initform nil)
   (value-start :initarg :value-start :initform 0)
   (value-end :initarg :value-end :initform nil)
   (key :initarg :key :type buffer-bytes)
   (value :initarg :value :type buffer-bytes)
   (next :initarg :next)))

(defclass free-block ()
  ((offset :initarg :offset)
   (size :initarg :size)))

(defclass skip-list-db (basic-db)
  ((stream :initform nil)
   (mmap-size :initarg :mmap-size :initform (ash 64 20))))

(defmethod db-open ((db skip-list-db) path)
  (with-slots (stream mmap-size) db
    (setf stream (open path :direction :io :element-type '(unsigned-byte 8)
                       :if-exists :overwrite :if-does-not-exist :create)
          stream (make-instance 'db-stream :base-stream stream :mmap-size mmap-size :ext 1.5))
    (unless (load-head db)
      (compute-head db)
      (dump-head db))))
