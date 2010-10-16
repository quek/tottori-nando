(in-package :tottori-nando.skip-list-db)

(defvar *sap*)

(defmacro with-sap ((file) &body body)
  `(let ((*sap* (slot-value ,file 'tottori-nando.internal::sap)))
     ,@body))

(defconstant +head-next-start-offset+ 0)
(defconstant +head-next-end-offset+ 8)
(defconstant +heap-start-offset+ 16)
(defconstant +heap-end-offset+ 24)
(defconstant +heap-fragments-offset+ 32)
(defconstant +node-count-offset+ 40)
(defconstant +initial-heap-start-value+ 64)

(defconstant +node-size+ (* 8 6))

(defstruct node
  (offset 0 :type fixnum)
  (key-start 0 :type fixnum)
  (key-end 0 :type fixnum)
  (value-start 0 :type fixnum)
  (value-end 0 :type fixnum)
  (next-start 0 :type fixnum)
  (next-end 0 :type fixnum))

(defun new-node (heap max-level kbuf ksiz vbuf vsiz)
  (let* ((next-size (* 8 max-level))
         (node-offset (alloc heap (+ +node-size+ ksiz)))
         (value-offset (alloc heap vsiz))
         (key-start (+ node-offset +node-size+))
         (key-end (+ key-start ksiz))
         (value-start value-offset)
         (value-end (+ value-start vsiz))
         (next-offset (alloc heap next-size))
         (next-start next-offset)
         (next-end (+ next-start next-size)))
    (setf (ref-64 *sap* node-offset) key-start
          (ref-64 *sap* (+ node-offset 8)) key-end
          (ref-64 *sap* (+ node-offset 16)) value-start
          (ref-64 *sap* (+ node-offset 24)) value-end
          (ref-64 *sap* (+ node-offset 32)) next-start
          (ref-64 *sap* (+ node-offset 40)) next-end)
    (copy-vector-to-sap kbuf 0 *sap* key-start ksiz)
    (copy-vector-to-sap vbuf 0 *sap* value-start vsiz)
    (loop for i from next-start below next-end by 8
          do (setf (ref-64 *sap* i) 0))
    (values (make-node :offset node-offset
                       :key-start key-start
                       :key-end key-end
                       :value-start value-start
                       :value-end value-end
                       :next-start next-start
                       :next-end next-end)
            node-offset)))

(defun free-node (heap node)
  (free heap (node-offset node))
  (free heap (node-value-start node))
  (free heap (node-next-start node)))

(defun save-node-value (node)
  (let ((offset (node-offset node)))
    (setf (ref-64 *sap* (+ offset 8 8))
          (node-value-start node)
          (ref-64 *sap* (+ offset 8 8 8))
          (node-value-end node))))

(defstruct free-block ()
  (offset 0 :type fixnum)
  (size 0 :type fixnum))

(defclass skip-list-db (basic-db)
  ((p :initarg :p :initform 0.25)
   (max-level :initform 1 :type fixnum :accessor max-level)
   (head :initform (make-node))
   (heap :initform (make-heap))
   (stream :initform nil)
   (mmap-size :initarg :mmap-size :initform (ash 32 20))
   (node-count :initform (make-instance 'atomic-int))
   (threshold-node-count :initform 0)
   (lock :initform (make-spinlock))))

(defmethod initialize-instance :after ((db skip-list-db) &key)
  (with-slots (p max-level) db
    (setf max-level (compute-max-level 100 p))))

(defun compute-max-level (record-count p)
  (max 1 (ceiling (log record-count (/ 1 p)))))

(defun recompute-max-level (skip-list-db node-count)
  (with-slots (p max-level threshold-node-count head heap) skip-list-db
    (when (< threshold-node-count node-count)
      (let ((new-max-level (compute-max-level node-count p)))
        (when (/= max-level new-max-level)
          (format t "~&new max level: ~d, old: ~d" new-max-level max-level)
          (let* ((new-next-size (* 8 new-max-level))
                 (old-head-next-start (node-next-start head))
                 (new-next (alloc heap new-next-size)))
            (loop for i from new-next below (+ new-next new-next-size) by 8
                  do (setf (ref-64 *sap* i) 0))
            (loop for src from (node-next-start head) below (node-next-end head) by 8
                  for dest from new-next by 8
                  do (setf (ref-64 *sap* dest) (ref-64 *sap* src)))
            (setf (node-next-start head) new-next
                  (node-next-end head) (+ new-next new-next-size))
            (save-head-next head)
            (setf threshold-node-count (compute-threshold-node-count skip-list-db))
            (setf max-level new-max-level)
            (free heap old-head-next-start)))))))

(defun compute-threshold-node-count (skip-list-db)
  (with-slots (node-count threshold-node-count) skip-list-db
    (let ((node-count (atomic-int-value node-count)))
      (setf threshold-node-count (ceiling (max (/ node-count 2) 10000))))))

(defun save-head-next (head-node)
  (setf (ref-64 *sap* +head-next-start-offset+) (node-next-start head-node)
        (ref-64 *sap* +head-next-end-offset+) (node-next-end head-node)))

(defmethod db-open ((db skip-list-db) path)
  (with-slots (stream mmap-size heap head max-level node-count) db
    (setf stream (open path :direction :io :element-type '(unsigned-byte 8)
                       :if-exists :overwrite :if-does-not-exist :create)
          stream (make-instance 'db-stream :base-stream stream :mmap-size mmap-size :ext 1.5))
    (with-sap (stream)
      (setf (heap-file heap) stream)
      (if (zerop (stream-length stream))
          (progn
            (stream-truncate stream mmap-size)
            (setf (heap-start heap) +initial-heap-start-value+
                  (heap-end heap) +initial-heap-start-value+
                  (heap-fragments-offset heap) 0)
            (let* ((size (* 8 max-level))
                   (next-start (alloc heap size)))
              (setf (node-next-start head) next-start
                    (node-next-end head) (+ next-start size))
              (save-head-next head)))
          (progn
            (setf (heap-start heap) (ref-64 *sap* +heap-start-offset+)
                  (heap-end heap) (ref-64 *sap* +heap-end-offset+)
                  (heap-fragments-offset heap) (ref-64 *sap* +heap-fragments-offset+))
            (load-fragments heap)
            (setf (node-next-start head) (ref-64 *sap* +head-next-start-offset+)
                  (node-next-end head) (ref-64 *sap* +head-next-end-offset+))
            (setf max-level (/ (- (node-next-end head) (node-next-start head)) 8))
            (setf (atomic-int-value node-count) (ref-64 *sap* +node-count-offset+)))))
    (compute-threshold-node-count db)))

(defmethod db-close ((db skip-list-db))
  (with-slots (head heap stream node-count) db
    (with-sap (stream)
      (dump-fragments heap)
      (setf (ref-64 *sap* +head-next-start-offset+) (node-next-start head)
            (ref-64 *sap* +head-next-end-offset+) (node-next-end head))
      (setf (ref-64 *sap* +heap-start-offset+) (heap-start heap)
            (ref-64 *sap* +heap-end-offset+) (heap-end heap)
            (ref-64 *sap* +heap-fragments-offset+) (heap-fragments-offset heap))
      (setf (ref-64 *sap* +node-count-offset+) (atomic-int-value node-count)))
    (close stream)))

(defun next-node (node level)
  (let ((offset (ref-64 *sap* (+ (node-next-start node) (* 8 level)))))
    (unless (zerop offset)
      (make-node :offset offset
                 :key-start (ref-64 *sap* offset)
                 :key-end (ref-64 *sap* (+ offset 8))
                 :value-start (ref-64 *sap* (+ offset 16))
                 :value-end (ref-64 *sap* (+ offset 24))
                 :next-start (ref-64 *sap* (+ offset 32))
                 :next-end (ref-64 *sap* (+ offset 40))))))

(defun compare-key (buf1 start1 end1 buf2 start2 end2)
  (loop for i1 from start1 below end1
        and i2 from start2 below end2
        for x = (- (gref buf1 i1) (gref buf2 i2))
        unless (zerop x)
          do (return-from compare-key x))
  (- (- end1 start1) (- end2 start2)))

(defun %skip-list-search (skip-list-db kbuf ksiz)
  (with-slots (head max-level) skip-list-db
    (loop with level fixnum = (1- max-level)
          with node-1 = head
          with prevs = (make-array max-level :initial-element nil)
          for node = (next-node head level) then (next-node node-1 level)
          if node
            do (let ((compare (compare-key *sap* (node-key-start node) (node-key-end node)
                                           kbuf 0 ksiz)))
                 (cond ((< compare 0)
                        (setf node-1 node))
                       ((< 0 compare)
                        (setf (aref prevs level) node-1)
                        (when (= -1 (decf level))
                          (return (values nil prevs))))
                       (t
                        (setf (aref prevs level) node-1)
                        (return (values node prevs level node-1)))))
          else
            do (setf (aref prevs level) node-1)
               (when (= -1 (decf level))
                 (return (values nil prevs))))))

(defun compute-level-for-add (p max-level)
  (loop for i from 1
        if (= i max-level)
          do (return max-level)
        if (< p (random 1.0))
          do (return i)))


(defun %skip-list-add (skip-list-db prevs kbuf ksiz vbuf vsiz)
  (with-slots (heap max-level p) skip-list-db
    (let ((max-level max-level))
      (multiple-value-bind (node offset) (new-node heap max-level kbuf ksiz vbuf vsiz)
        (loop for level fixnum from 0 below (compute-level-for-add p max-level)
              for prev across (the simple-vector prevs)
              do (shiftf (ref-64 *sap* (+ (node-next-start node) (* 8 level)))
                         (ref-64 *sap* (+ (node-next-start prev) (* 8 level)))
                         offset))))))

(defun %replace-value (skip-list-db node vbuf vsiz)
  (with-slots (heap) skip-list-db
    (let ((value-offset (alloc heap vsiz))
          (old-node-value-start (node-value-start node)))
      (copy-vector-to-sap vbuf 0
                          *sap* value-offset
                          vsiz)
      (setf (node-value-start node) value-offset
            (node-value-end node) (+ value-offset vsiz))
      (save-node-value node)
      (free heap old-node-value-start))))

(defun %skip-list-remove (skip-list-db node level prev)
  (with-slots (heap max-level) skip-list-db
    (loop for i from level downto 0
          do (loop until (= (node-offset node) (node-offset (next-node prev i)))
                   do (setf prev (next-node prev i))
                   finally (setf (ref-64 *sap* (+ (node-next-start prev) (* 8 i)))
                                 (ref-64 *sap* (+ (node-next-start node) (* 8 i))))))
    (free-node heap node)))


(defmethod accept ((db skip-list-db) kbuf ksiz writable full empty)
  (with-slots (head stream node-count lock) db
    (with-spinlock (lock)
      (with-sap (stream)
        (if writable
            ;; 更新系
            (multiple-value-bind (node prevs level prev) (%skip-list-search db kbuf ksiz)
              (if node
                  ;; 該当あり
                  (multiple-value-bind (vbuf vsiz) (funcall full kbuf ksiz
                                                            node 0)
                    (case vbuf
                      (:remove
                         ;; 削除
                         (%skip-list-remove db node level prev)
                         (atomic-int-add node-count -1))
                      (:nop t)
                      (t
                         ;; 置き換え（+nop+ ではない場合）
                         (%replace-value db node vbuf vsiz))))
                  ;; 該当なし
                  (multiple-value-bind (vbuf vsiz) (funcall empty kbuf ksiz)
                    (case vbuf
                      ((:nop :remove) t)
                      (t
                         (%skip-list-add db prevs kbuf ksiz vbuf vsiz)
                         (recompute-max-level db (atomic-int-add node-count 1)))))))
            ;; 参照系
            (let ((node (%skip-list-search db kbuf ksiz)))
              (if node
                  (let* ((vsiz (- (node-value-end node) (node-value-start node)))
                         (vbuf (make-buffer vsiz)))
                    (copy-sap-to-vector *sap* (node-value-start node) vbuf 0 vsiz)
                    (funcall full kbuf ksiz vbuf vsiz))
                  (funcall empty kbuf ksiz))))))))

(defun test1 ()
  (let ((db (make-instance 'skip-list-db)))
    (db-open db "/tmp/skip-list-test1.db")
    (unwind-protect
         (progn
           (setf (value db "foo") "hop")
           (setf (value db "bar") "step")
           (setf (value db "baz") "jump")
           (assert (equal "hop" (print (value db "foo"))))
           (assert (equal "step" (print (value db "bar"))))
           (assert (equal "jump" (print (value db "baz"))))
           (setf (value db "bar") "ばー")
           (assert (equal "ばー" (print (value db "bar"))))
           (delete-op db "bar")
           (assert (null (value db "bar")))
           (setf (value db "aaa") "a")
           (setf (value db "aaa") "aa")
           (value db "aaa")
           (= 3 (atomic-int-value (slot-value db 'node-count))))
      (db-close db))))

(defun test2 ()
  (time
   (let* ((n 100000)
          (db (make-instance 'skip-list-db)))
     (db-open db "/tmp/skip-list-test2.db")
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


(defun test3 ()
  (time
   (let* ((n 10000)
          (thread-count 3)
          (db (make-instance 'skip-list-db))
          (file "/tmp/skip-list-test3.db"))
     (ignore-errors (delete-file file))
     (db-open db file)
     (unwind-protect
          (let ((threads
                 (loop for i from 0 below thread-count collect
                   (let ((i i))
                     (sb-thread:make-thread
                      (lambda ()
                        (macrolet ((m (&body body)
                                     `(loop for x from (* i n) below (+ (* i n) n)
                                            for k = (format nil "key~a" x)
                                            for v = (format nil "value~a" x)
                                            do ,@body)))
                          (m (setf (value db k) v))
                          (m (assert (equal v (value db k)) nil
                                     "~a => ~a : ~a" k v (value db k))))))))))
            (mapc #'sb-thread:join-thread threads))
       (db-close db)))))