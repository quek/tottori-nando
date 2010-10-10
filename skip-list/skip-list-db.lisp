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

(defun free-node (heap node)
  (free heap (node-value-start node))
  (free heap (node-offset node)))

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
   (max-level :initarg :max-level :initform 0 :type fixnum :accessor max-level)
   (head :initform (make-instance 'node))
   (heap :initform (make-heap))
   (stream :initform nil)
   (mmap-size :initarg :mmap-size :initform (ash 32 20))))

(defun make-skip-list-db (record-count &key (p 0.25))
  (let ((db (make-instance 'skip-list-db :p p)))
    (with-slots (p max-level head) db
      (setf max-level (ceiling (log record-count (/ 1 p)))
            head (make-node)))
    db))


(defmethod db-open ((db skip-list-db) path)
  (with-slots (stream mmap-size heap head max-level) db
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
                      (node-next-end head) (+ next-start size))))
          (progn
            (setf (heap-start heap) (ref-64 *sap* +heap-start-offset+)
                  (heap-end heap) (ref-64 *sap* +heap-end-offset+)
                  (heap-fragments-offset heap) (ref-64 *sap* +heap-fragments-offset+))
            (load-fragments heap)
            (setf (node-next-start head) (ref-64 *sap* +head-next-start-offset+)
                  (node-next-end head) (ref-64 *sap* +head-next-end-offset+)))))))

(defmethod db-close ((db skip-list-db))
  (with-slots (head heap stream) db
    (with-sap (stream)
      (dump-fragments heap)
      (setf (ref-64 *sap* +head-next-start-offset+) (node-next-start head)
            (ref-64 *sap* +head-next-end-offset+) (node-next-end head))
      (setf (ref-64 *sap* +heap-start-offset+) (heap-start heap)
            (ref-64 *sap* +heap-end-offset+) (heap-end heap)
            (ref-64 *sap* +heap-fragments-offset+) (heap-fragments-offset heap)))
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
          for node = (next-node head level)
            then (next-node node-1 level)
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

(defun compute-level-for-add (skip-list-db)
  (with-slots (p max-level) skip-list-db
    (loop for i from 1
          if (= i max-level)
            do (return max-level)
          if (< p (random 1.0))
            do (return i))))

(defun new-node (skip-list-db kbuf ksiz vbuf vsiz)
  (with-slots (heap max-level) skip-list-db
    (let* ((next-size (* 8 max-level))
           (node-offset (alloc heap (+ +node-size+ ksiz next-size)))
           (value-offset (alloc heap vsiz))
           (key-start (+ node-offset +node-size+))
           (key-end (+ key-start ksiz))
           (value-start value-offset)
           (value-end (+ value-start vsiz))
           (next-start key-end)
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
              node-offset))))

(defun %skip-list-add (skip-list-db prevs kbuf ksiz vbuf vsiz)
  (with-slots (heap max-level) skip-list-db
    (multiple-value-bind (node offset) (new-node skip-list-db kbuf ksiz vbuf vsiz)
      (loop for level fixnum from 0 below (compute-level-for-add skip-list-db)
            for prev across (the simple-vector prevs)
            do (shiftf (ref-64 *sap* (+ (node-next-start node) (* 8 level)))
                       (ref-64 *sap* (+ (node-next-start prev) (* 8 level)))
                       offset)))))

(defun %replace-value (skip-list-db node vbuf vsiz)
  (with-slots (heap) skip-list-db
    (let ((value-offset (alloc heap vsiz))
          (old (node-value-start node)))
      (copy-vector-to-sap vbuf 0
                          *sap* value-offset
                          vsiz)
      (setf (node-value-start node) value-offset
            (node-value-end node) (+ value-offset vsiz))
      (save-node-value node)
      (free heap old))))

(defun %skip-list-remove (skip-list-db node level prev)
  (with-slots (heap max-level) skip-list-db
    (loop for i from level downto 0
          do (loop until (= (node-offset node) (node-offset (next-node prev i)))
                   do (setf prev (next-node prev i))
                   finally (setf (ref-64 *sap* (+ (node-next-start prev) (* 8 i)))
                                 (ref-64 *sap* (+ (node-next-start node) (* 8 i))))))
    (free-node heap node)))

(defmethod accept ((db skip-list-db) kbuf ksiz writable full empty)
  (with-slots (head stream) db
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
                       (%skip-list-remove db node level prev))
                    (:nop t)
                    (t
                       ;; 置き換え（+nop+ ではない場合）
                       (%replace-value db node vbuf vsiz))))
                ;; 該当なし
                (multiple-value-bind (vbuf vsiz) (funcall empty kbuf ksiz)
                  (case vbuf
                    ((:nop :remove) t)
                    (t
                       (%skip-list-add db prevs kbuf ksiz vbuf vsiz))))))
          ;; 参照系
          (let ((node (%skip-list-search db kbuf ksiz)))
            (if node
                (let* ((vsiz (- (node-value-end node) (node-value-start node)))
                       (vbuf (make-buffer vsiz)))
                  (copy-sap-to-vector *sap* (node-value-start node) vbuf 0 vsiz)
                  (funcall full kbuf ksiz vbuf vsiz))
                (funcall empty kbuf ksiz)))))))

(defun test ()
  (let ((db (make-skip-list-db 100)))
    (db-open db "/tmp/s.db")
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
           (value db "aaa"))
      (db-close db))))
