#|
kcfile のトランザクションの実装は
ファイルに変更前を書く。
ファイル出力。
コミットなならログ破棄。
アボートならログをファイルに反映して元に戻す。
なのかな。
|#
(in-package :tottori-nando.internal)

(defclass mmap-stream (sb-gray:fundamental-binary-input-stream
                     sb-gray:fundamental-binary-output-stream)
  ((base-stream :initarg :base-stream)
   (mmap-size :initarg :mmap-size)
   (file-length :initform 0)
   (position :initform 0)
   (sap :reader mmap-stream-sap)
   (ext :initarg :ext :initform nil)))

(defmethod initialize-instance :after ((stream mmap-stream) &key)
  (with-slots (base-stream file-length mmap-size sap) stream
    (setf file-length (file-length base-stream)
          sap (sb-posix:mmap nil
                             mmap-size
                             (boole boole-ior sb-posix:prot-read sb-posix:prot-write)
                             sb-posix:map-shared
                             (sb-sys:fd-stream-fd base-stream)
                             0))))

(defmethod close ((stream mmap-stream) &key abort)
  (with-slots (base-stream mmap-size sap) stream
    (sb-posix:munmap sap mmap-size)
    (close base-stream :abort abort)))

(defmethod read-seq-at (stream sequence position &key (start 0) end)
  (file-position stream position)
  (read-sequence sequence stream :start start :end end))

(defmethod write-seq-at (stream sequence position &key (start 0) end)
  (file-position stream position)
  (write-sequence sequence stream :start start :end end))

(defmacro define-write-method (name
                               (&rest lambda-list)
                               length
                               <proc
                               <=proc
                               tproc
                               return-value)
  `(defmethod ,name ,lambda-list
     (with-slots (base-stream file-length mmap-size sap position ext) stream
       (let* ((length ,length)
              (end-position (+ length position)))
         (flet ((ext ()
                  (let ((current-len file-length))
                    (when (< current-len end-position)
                      (stream-truncate stream
                                       (if ext
                                           (min mmap-size (ceiling (* current-len ext)))
                                           end-position))))))
           (cond ((< end-position mmap-size)
                  (ext)
                  ,<proc)
                 ((<= mmap-size position)
                  ,<=proc)
                 (t
                  (ext)
                  ,tproc))
           (setf position end-position)
           ,return-value)))))

(defmacro define-read-method (name
                              (&rest lambda-list)
                              length
                              <proc
                              <=proc
                              tproc)
  `(defmethod ,name ,lambda-list
     (with-slots (base-stream mmap-size sap position) stream
       (let ((length ,length))
         (prog1
             (cond ((< (+ position length) mmap-size)
                    ,<proc)
                   ((<= mmap-size position)
                    ,<=proc)
                   (t
                    ,tproc))
           (incf position length))))))

(define-write-method sb-gray:stream-write-sequence ((stream mmap-stream)
                                                    (buffer sequence)
                                                    &optional (start 0) end)
  (if end (- end start) (length buffer))
  (copy-vector-to-sap buffer start sap position length)
  (progn
    (file-position base-stream position)
    (write-sequence buffer base-stream)
    (setf file-length end-position))
  (let ((mlen (- mmap-size position)))
    (copy-vector-to-sap buffer start sap position mlen)
    (file-position base-stream (1- mmap-size))
    (write-sequence buffer base-stream :start (1- mlen) :end end))
  buffer)


(define-read-method sb-gray:stream-read-sequence ((stream mmap-stream)
                                                  (buffer sequence)
                                                  &optional (start 0) end)
  (progn
    (unless end (setf end (length buffer)))
    (- end start))
  (progn
    (copy-sap-to-vector sap position buffer start length)
    end)
  (progn
    (file-position base-stream position)
    (read-sequence buffer base-stream))
  (let ((mlen (- mmap-size position)))
    (copy-sap-to-vector sap position buffer start mlen)
    (file-position base-stream (1- mmap-size))
    (read-sequence buffer base-stream :start (1- mlen) :end end)))

(define-write-method sb-gray:stream-write-byte ((stream mmap-stream) integer)
  1
  (setf (sb-sys:sap-ref-8 sap position) integer)
  (progn
    (file-position base-stream position)
    (write-byte integer base-stream))
  ()
  integer)

(define-read-method sb-gray:stream-read-byte ((stream mmap-stream))
  1
  (sb-sys:sap-ref-8 sap position)
  (progn
    (file-position base-stream position)
    (read-byte base-stream))
  ())

(defmethod sb-gray:stream-file-position ((stream mmap-stream) &optional position-spec)
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

(defmethod stream-truncate ((stream mmap-stream) size)
  (with-slots (base-stream file-length) stream
    (setf file-length size)
    (sb-posix:ftruncate base-stream size)))

(defmethod stream-length ((stream mmap-stream))
  (with-slots (file-length) stream
    file-length))

#|
(let ((f (open "/tmp/a.txt" :direction :io :element-type '(unsigned-byte 8)
               :if-exists :overwrite :if-does-not-exist :create)))
  (with-open-stream (m (make-instance 'mmap-stream :base-stream f :mmap-size 5))
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
