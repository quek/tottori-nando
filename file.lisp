(in-package :tottori-nando)

(defclass db-stream (sb-gray:fundamental-binary-input-stream
                     sb-gray:fundamental-binary-output-stream)
  ((base-stream :initarg :base-stream)
   (mmap-size :initarg :mmap-size)
   (position :initform 0)
   (sap)))

(defmethod initialize-instance :after ((stream db-stream) &key)
  (with-slots (base-stream mmap-size sap) stream
    (setf sap (sb-posix:mmap nil
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
    (let* ((length (if end (- end start) (length buffer))))
      (flet ((ext ()
               (when (< (file-length base-stream) (+ position length))
                 (sb-posix:ftruncate base-stream (+ position length)))))
        (cond ((< (+ position length) mmap-size)
               (ext)
               (sb-sys::with-pinned-objects (sap buffer)
                 (sb-kernel::system-area-ub8-copy (sb-sys::vector-sap buffer) start
                                                  sap position
                                                  length)))
              ((<= mmap-size position)
               (file-position base-stream position)
               (write-sequence buffer base-stream))
              (t
               (ext)
               (let ((mlen (- mmap-size position)))
                 (sb-sys::with-pinned-objects (sap buffer)
                   (sb-kernel::system-area-ub8-copy (sb-sys::vector-sap buffer) start
                                                    sap position
                                                    mlen))
                 (file-position base-stream (1- mmap-size))
                 (write-sequence buffer base-stream :start (1- mlen) :end end))))
        (setf position (+ position length))
        buffer))))

(defmethod sb-gray:stream-read-sequence ((stream db-stream)
                                         (buffer sequence)
                                         &optional (start 0) (end nil))
  (with-slots (base-stream mmap-size sap position) stream
    (unless end (setf end (length buffer)))
    (let ((length (- end start)))
      (cond ((< (+ position length) mmap-size)
             (sb-sys::with-pinned-objects (sap buffer)
               (sb-kernel::system-area-ub8-copy sap position
                                                (sb-sys::vector-sap buffer) start
                                                length)))
            ((<= mmap-size position)
             (file-position base-stream position)
             (read-sequence buffer base-stream))
            (t
             (let ((mlen (- mmap-size position)))
               (sb-sys::with-pinned-objects (sap buffer)
                 (sb-kernel::system-area-ub8-copy sap position
                                                  (sb-sys::vector-sap buffer) start
                                                  mlen))
               (file-position base-stream (1- mmap-size))
               (read-sequence buffer base-stream :start (1- mlen) :end end))))
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
  (with-slots (base-stream mmap-size sap position) stream
    (if (< position mmap-size)
        (setf (sb-sys:sap-ref-8 sap position) integer)
        (progn
          (file-position base-stream position)
          (write-byte integer base-stream)))
    (incf position)
    integer))

(defmethod sb-gray:stream-read-byte ((stream db-stream))
  (with-slots (base-stream mmap-size sap position) stream
    (prog1
        (if (< position mmap-size)
            (sb-sys:sap-ref-8 sap position)
            (progn
              (file-position base-stream position)
              (read-byte base-stream)))
      (incf position))))

(defmethod stream-truncate ((stream db-stream) size)
  (with-slots (base-stream) stream
    (sb-posix:ftruncate base-stream size)))

(defmethod stream-length ((stream db-stream))
  (with-slots (base-stream) stream
    (file-length base-stream)))

#|
(let ((f (open "/tmp/a.txt" :direction :io :element-type '(unsigned-byte 8)
               :if-exists :overwrite :if-does-not-exist :create)))
  (with-open-stream (m (make-instance 'db-stream :base-stream f :mmap-size 5))
    (stream-truncate m 7)
    (let ((code (char-code #\!)))
      (file-position m 1)
      (write-byte code m)
      (file-position m 1)
      (assert (= code (read-byte m))))
    (let ((buffer (make-array 3 :element-type '(unsigned-byte 8))))
      (file-position m 0)
      (write-sequence (string-to-octets "abc") m)
      (file-position m 0)
      (read-sequence buffer m)
      (assert (equalp (string-to-octets "abc") buffer) (buffer) "1 ~a" buffer))
    (let ((buffer (make-array 3 :element-type '(unsigned-byte 8))))
      (write-sequence (string-to-octets "def") m)
      (file-position m 3)
      (read-sequence buffer m)
      (assert (equalp (string-to-octets "def") buffer) (buffer) "2 ~a" buffer))
    (let ((buffer (make-array 3 :element-type '(unsigned-byte 8))))
      (write-sequence (string-to-octets "ghi") m)
      (file-position m 6)
      (read-sequence buffer m)
      (assert (equalp (string-to-octets "ghi") buffer) (buffer) "3 ~a" buffer))
    (let ((code (char-code #\#)))
      (write-byte code m)
      (file-position m (1- (file-position m)))
      (assert (= code (read-byte m))))
    (let ((buffer1 (make-array 20 :element-type '(unsigned-byte 8) :initial-element 1))
          (buffer2 (make-array 20 :element-type '(unsigned-byte 8))))
      (file-position m 0)
      (write-sequence buffer1 m)
      (file-position m 0)
      (read-sequence buffer2 m)
      (assert (equalp buffer1 buffer2) (buffer1 buffer2) "3 ~a" buffer))))
|#
