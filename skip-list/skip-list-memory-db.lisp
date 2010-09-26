(in-package :tottori-nando.skip-list-memory-db)

(defun vector< (a b)
  (loop for x across a
        and y across b
        if (< x y)
          do (return-from vector< t)
        if (< y x)
          do (return-from vector< nil))
  (< (length a) (length b)))

(defstruct node
  (key nil)
  (val nil)
  (next #() :type simple-vector))

(defclass skip-list ()
  ((p :initarg :p :initform 0.25)
   (max-level :initarg :max-level :initform 0 :type fixnum :accessor max-level)
   (head :initarg :head :accessor head)
   (key< :initarg :key<)
   (mlock_ :initform (make-instance 'spin-rw-lock))))

(defun make-skip-list (record-count &key (p 0.25) (key< #'vector<))
  (let ((list (make-instance 'skip-list :p p :key< key<)))
    (with-slots (p max-level head) list
      (setf max-level (ceiling (log record-count (/ 1 p)))
            head (make-node :next (make-array max-level :initial-element nil))))
    list))

(defmethod print-object ((list skip-list) stream)
  (print-unreadable-object (list stream :type t :identity t)
    (format stream "~a" (head list))))

(defmethod dump-skip-list ((list skip-list))
  (loop for level from 0 below (max-level list)
        do (format t "~&~d " level)
        do (loop for x = (head list) then i
                 for i = (aref (node-next x) level)
                 while i
                 do (progn
                      (format t "~a " i)))))

(defmacro |skip-list-search'| (with-prevs add-prevs return-nil return-node)
  `(loop with max-level fixnum = (slot-value skip-list 'max-level)
        with key< = (slot-value skip-list 'key<)
        with level fixnum = (1- max-level)
        with node-1 = (head skip-list)
        ,@with-prevs
        for node = (aref (node-next (head skip-list)) level)
          then (aref (node-next node-1) level)
        if node
          do (let ((node-key (node-key node)))
               (cond ((funcall key< node-key key)
                      (setf node-1 node))
                     ((funcall key< key node-key)
                      #1=(progn
                           ,add-prevs
                           (when (= -1 (decf level))
                             ,return-nil)))
                     (t
                      ,add-prevs
                      ,return-node)))
        else
          do #1#))

(defun %skip-list-search (skip-list key)
  (declare (optimize (speed 3) (safety 0)))
  (|skip-list-search'| (with prevs = (make-array max-level :initial-element nil))
                       (setf (aref prevs level) node-1)
                       (return (values nil prevs))
                       (return (values node prevs))))

(defun %%skip-list-search (skip-list key)
  (declare (optimize (speed 3) (safety 0)))
  (|skip-list-search'| nil
                       nil
                       (return nil)
                       (return node)))

(defun skip-list-search (skip-list key)
  (let ((node (%%skip-list-search skip-list key)))
    (if node
        (values (node-val node) t)
        (values nil nil))))

(defun compute-level-for-add (skip-list)
  (with-slots (p max-level) skip-list
    (loop for i from 1
          if (= i max-level)
            do (return max-level)
          if (< p (random 1.0))
            do (return i))))

(defun %skip-list-add (skip-list prevs key val)
  (let ((new-node (make-node :key key :val val
                             :next (make-array (the fixnum (max-level skip-list))
                                               :initial-element nil)))
        (top-level (compute-level-for-add skip-list)))
    (loop for level fixnum from 0 below top-level
          for prev across (the simple-vector prevs)
          do (shiftf (aref (the simple-vector (node-next new-node)) level)
                     (aref (the simple-vector (node-next prev)) level)
                     new-node))))

(defun skip-list-add (skip-list key val)
  (declare (optimize (speed 3) (safety 0)))
  (multiple-value-bind (node prevs) (%skip-list-search skip-list key)
    (if node
        (setf (node-val node) val)
        (%skip-list-add skip-list prevs key val))))

;;     3     6
;; 1   3   5 6
;; 1 2 3 4 5 6
(defun %skip-list-remove (node prevs)
  (loop for level from 0
        for prev across prevs
        if prev
          do (progn
               (print (list level prev))
               (loop for i from level downto 0
                   do (print prev)
                   do (loop until (eq node (aref (node-next prev) i))
                            do (setf prev (aref (node-next prev) i))
                            finally (setf (aref (node-next prev) i)
                                          (aref (node-next node) i))))
               (return t))))

(defun skip-list-remove (skip-list key)
  (multiple-value-bind (node prevs) (%skip-list-search skip-list key)
    (when node
      (%skip-list-remove node prevs)
      node)))

(defclass skip-list-memory-db (basic-db)
  ((mlock_ :initform (make-instance 'spin-rw-lock))
   (skip-list)
   (record-count :initargs :record-count :initform 100000)))

(defmethod initialize-instance :after ((db skip-list-memory-db) &key)
  (with-slots (skip-list record-count) db
    (setf skip-list (make-skip-list record-count))))

(defmethod accept ((db skip-list-memory-db) kbuf ksiz writable full empty)
  (with-slots (skip-list mlock_) db
    (if writable
        ;; 更新系
        (with-spin-rw-lock (mlock_ t)
          (multiple-value-bind (node prevs) (%skip-list-search skip-list kbuf)
            (if node
                ;; 該当あり
                (multiple-value-bind (vbuf vsiz) (funcall full kbuf ksiz
                                                          (node-val node)
                                                          (length (node-val node)))
                  (declare (ignore vsiz))
                  (case vbuf
                    (:remove
                       ;; 削除
                       (%skip-list-remove node prevs))
                    (:nop t)
                    (t
                       ;; 置き換え（+nop+ ではない場合）
                       (setf (node-val node) vbuf))))
                ;; 該当なし
                (multiple-value-bind (vbuf vsiz) (funcall empty kbuf ksiz)
                  (declare (ignore vsiz))
                  (case vbuf
                    ((:nop :remove) t)
                    (t
                       (%skip-list-add skip-list prevs kbuf vbuf)))))))
        ;; 参照系
        (with-spin-rw-lock (mlock_ nil)
          (let ((node (%skip-list-search skip-list kbuf)))
            (if node
                (funcall full kbuf ksiz (node-val node) (length (node-val node)))
                (funcall empty kbuf ksiz)))))))


(defun test ()
  (let ((db (make-instance 'skip-list-memory-db)))
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
    (value db "aaa"))
  )
