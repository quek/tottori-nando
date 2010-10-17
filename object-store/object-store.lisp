(in-package :tottori-nando.object-store)

(defstruct node
  (offset 0 :type fixnum)
  (key-start 0 :type fixnum)
  (key-end 0 :type fixnum)
  (value-start 0 :type fixnum)
  (value-end 0 :type fixnum)
  (next-start 0 :type fixnum)
  (next-end 0 :type fixnum))

(defclass skip-list ()
  ((heap :initarg :heap)
   (p :initarg :p :initform 0.25)
   (max-level :initarg :max-level :initform 1 :type fixnum :accessor max-level)
   (head :initarg :head :initform (make-node))
   (node-count :initarg :node-count :initform (make-instance 'atomic-int))
   (threshold-node-count :initarg :threshold-node-count :initform 0)
   (lock :initform (make-instance 'spin-rw-lock))))

(defclass store ()
  ((mmap-size :initarg :mmap-size :initform (ash 32 20))
   (stream)
   (path)
   (file-extent :initarg :file-extent :initform 1.5)
   (heap)
   (root :type skip-list)
   (lock :initform (make-instance 'spin-rw-lock))))

(defconstant +heap-start-offset+ 16)
(defconstant +heap-end-offset+ 24)
(defconstant +root-skip-list-p-offset+ 64)
(defconstant +root-skip-list-max-level-offset+ (+ +root-skip-list-p-offset+ 8))
(defconstant +root-skip-list-head-offset+ (+ +root-skip-list-max-level-offset+ 8))
(defconstant +root-skip-list-node-count-offset+ (+ +root-skip-list-head-offset+ 16))
(defconstant +root-skip-list-threshold-node-count-offset+ (+ +root-skip-list-node-count-offset+ 8))


(defun make-root-head-node (root)
  (with-slots (heap max-level) root
    (let* ((size (* 8 max-level))
           (next-start (alloc heap size)))
      (make-node :next-start next-start
                 :next-end (+ next-start size)))))

(defun load-root-head-node (store)
  (with-slots (stream) store
    (make-node :next-start (read-64-at stream +root-skip-list-head-offset+)
               :next-end (read-64-at stream (+ +root-skip-list-head-offset+ 8)))))

(defun save-root-head-node (store)
  (with-slots (root stream) store
    (let ((head (slot-value root 'head)))
      (write-64-at stream +root-skip-list-head-offset+ (node-next-start head))
      (write-64-at stream (+ +root-skip-list-head-offset+ 8) (node-next-end head)))))

(defun make-root-skip-list (store)
  (with-slots (heap) store
    (let ((root (make-instance 'skip-list :heap heap)))
      (with-slots (head) root
        (setf head (make-root-head-node root)))
      root)))

(defun save-root-skip-list (store)
  (with-slots (root stream) store
    (with-slots (p max-level node-count threshold-node-count) root
      (write-64-at stream +root-skip-list-p-offset+ (truncate (* p 1000)))
      (write-64-at stream +root-skip-list-max-level-offset+ max-level)
      (save-root-head-node store)
      (write-64-at stream +root-skip-list-node-count-offset+ (atomic-int-value node-count))
      (write-64-at stream +root-skip-list-threshold-node-count-offset+ threshold-node-count))))

(defun load-root-skip-list (store)
  (with-slots (heap root stream) store
    (let ((p (read-64-at stream +root-skip-list-p-offset+))
          (max-level (read-64-at stream +root-skip-list-max-level-offset+))
          (node-count (read-64-at stream +root-skip-list-node-count-offset+))
          (threshold-node-count (read-64-at stream +root-skip-list-threshold-node-count-offset+)))
      (setf root
            (make-instance 'skip-list
                           :heap heap
                           :p (/ p 1000.0)
                           :max-level max-level
                           :head (load-root-head-node store)
                           :node-count (make-instance 'atomic-int :value node-count)
                           :threshold-node-count threshold-node-count)))))

(defun load-heap (store)
  (with-slots (heap stream) store
    (setf heap (make-heap :start (read-64-at stream +heap-start-offset+)
                          :end (read-64-at stream +heap-end-offset+)
                          :stream stream))
    (load-fragments heap)))

(defun save-heap (store)
  (with-slots (heap stream) store
    (write-64-at stream +heap-start-offset+ (heap-start heap))
    (write-64-at stream +heap-end-offset+ (heap-end heap))
    (dump-fragments heap)))

(defun initialize-heap (store)
  (with-slots (heap stream) store
    (setf heap (make-heap :start 1024 :end 1024 :stream stream))))

(defun initialize-store (store)
  (with-slots (root stream) store
    (stream-truncate stream 1024)
    (write-sequence (string-to-octets "sawako") stream)
    (initialize-heap store)
    (setf root (make-root-skip-list store))
    (save-root-skip-list store)))

(defun load-store (store)
  (with-slots (root stream) store
    (let ((buffer (make-buffer 6)))
      (read-seq-at stream buffer 0)
      (unless (string= "sawako" (octets-to-string buffer))
        (error "このファイルは違う。")))
    (load-heap store)
    (setf root (load-root-skip-list store))))

(defun save-store (store)
  (save-root-skip-list store)
  (save-heap store))

(defun open-store (store path)
  (with-slots (file-extent mmap-size root-skip-list stream) store
    (setf (slot-value store 'path) path)
    (setf stream (open-mmap-stream path mmap-size file-extent))
    (if (zerop (stream-length stream))
        (initialize-store store)
        (load-store store))))

(defun close-store (store)
  (with-slots (lock stream) store
    (with-spin-rw-lock (lock t)
      (save-store store)
      (close stream))))


(defun test1 ()
  (let ((store (make-instance 'store)))
    (open-store store "/tmp/store")
    (unwind-protect
         ()
      (close-store store))))
