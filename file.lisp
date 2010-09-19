(in-package :tottori-nando)

(defclass db-stream (sb-gray:fundamental-binary-input-stream
                     sb-gray:fundamental-binary-output-stream)
  ((base-stream :initarg :base-stream)
   (mmap-size :initarg :mmap-size)
   (position :initform 0)
   (sap)))

(defmethod initialize-instance :after ((stream db-stream) &key)
  (with-slots (base-stream mmap-size sap) stream
    (setf sap (sb-posix:mmap nil ; where to map (NIL if you don't care)?
                             mmap-size
                             (boole boole-ior sb-posix:prot-read sb-posix:prot-write)
                             sb-posix:map-shared
                             (sb-sys:fd-stream-fd base-stream)
                             0))))

(defmethod close ((stream db-stream) &key abort)
  (with-slots (base-stream mmap-size sap) stream
    (sb-posix:munmap sap mmap-size)
    (close base-stream :abort abort)))

(defmethod sb-gray:stream-write-sequence ((stream db-stream)
                                          (buffer sequence)
                                          &optional (start 0) end)
  (with-slots (base-stream mmap-size sap position) stream
    (let ((length (if end (- end start) (length buffer))))
      (sb-sys::with-pinned-objects (sap buffer)
        (sb-kernel::system-area-ub8-copy (sb-sys::vector-sap buffer) start
                                         sap position
                                         length))
      (incf position length)
      buffer)))

(defmethod sb-gray:stream-read-sequence ((stream db-stream)
                                         (buffer sequence)
                                         &optional (start 0) (end nil))
  (with-slots (base-stream mmap-size sap position) stream
    (unless end (setf end (length buffer)))
    (let ((length (- end start)))
      (sb-sys::with-pinned-objects (sap buffer)
        (sb-kernel::system-area-ub8-copy sap position
                                         (sb-sys::vector-sap buffer) start
                                         length))
      (incf position length)
      end)))


(defmethod sb-gray:stream-file-position ((stream db-stream) &optional position-spec)
  (with-slots (base-stream position) stream
    (if position-spec
        (setf position
              (case position-spec
                (:start 0)
                (:end
                   (file-position base-stream :end)
                   (file-position base-stream))
                (t position-spec)))
        position)))

(defmethod sb-gray:stream-write-byte ((stream db-stream) integer)
  (with-slots (sap position) stream
    (setf (sb-sys:sap-ref-8 sap position) integer)
    (incf position)
    integer))

(defmethod sb-gray:stream-read-byte ((stream db-stream))
  (with-slots (sap position) stream
    (prog1 (sb-sys:sap-ref-8 sap position)
      (incf position))))


(defmethod stream-truncate ((stream db-stream) size)
  (with-slots (base-stream) stream
    (sb-posix:ftruncate base-stream size)))

#|
(setf f (open "/tmp/a.txt" :direction :io :element-type '(unsigned-byte 8)
              :if-exists :overwrite :if-does-not-exist :create))
(setf m (make-instance 'db-stream :base-stream f :mmap-size 100))
(slot-value m 'sap)
;; => #.(SB-SYS:INT-SAP #X7FFFF7FF1000)
(stream-truncate m 10)
(sb-sys:sap-ref-8 (slot-value m 'sap) 0)
(setf (sb-sys:sap-ref-8 (slot-value m 'sap) 0) 99)
(progn
  (file-position m 0)
  (write-sequence (string-to-octets "!@#") m))
(sb-gray:stream-write-sequence m (string-to-octets "abc"))
(let ((buffer (make-array 10 :element-type '(unsigned-byte 8))))
  (file-position m 0)
  (values (read-sequence buffer m) buffer))
(progn
  (file-position m 0)
  (write-byte 26 m)
  (file-position m 0)
  (read-byte m))
(close m)
|#
