(in-package :tottori-nando.internal)

(defun make-spinlock ()
  (cons nil nil))

(defun lock-spinlock (spinlock)
  (loop while (sb-ext:compare-and-swap (car spinlock) nil t)))

(defun unlock-spinlock (spinlock)
  (setf (car spinlock) nil))

(defmacro with-spinlock ((spinlock) &body body)
  (alexandria:once-only (spinlock)
    `(progn
       (lock-spinlock ,spinlock)
       (unwind-protect
            (progn ,@body)
         (unlock-spinlock ,spinlock)))))

(defun make-recursive-spinlock ()
  (cons nil 0))

(defun lock-recursive-spinlock (recursive-spinlock)
  (loop with self = sb-thread:*current-thread*
    for ret = (sb-ext:compare-and-swap (car recursive-spinlock) nil self)
        until (or (null ret) (eq ret self))
        finally (incf (cdr recursive-spinlock))))

(defun unlock-recursive-spinlock (recursive-spinlock)
  (when (decf (cdr recursive-spinlock))
    (setf (car recursive-spinlock) nil)))

(defmacro with-recursive-spinlock ((recursive-spinlock) &body body)
  (alexandria:once-only (recursive-spinlock)
    `(progn
       (lock-recursive-spinlock ,recursive-spinlock)
       (unwind-protect
            (progn ,@body)
         (unlock-recursive-spinlock ,recursive-spinlock)))))


(defclass spin-rw-lock ()
  ((spinlock :initform (make-spinlock))
   (write-count :initform 0)
   (read-count :initform 0)))


(defgeneric lock-writer (lock))
(defgeneric lock-writer-try (lock))
(defgeneric lock-reader (lock))
(defgeneric lock-reader-try (lock))
(defgeneric unlock (lock))
(defgeneric promote (lock))
(defgeneric demote (lock))

(defmethod lock-writer ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (loop
      (with-spinlock (spinlock)
        (when (and (zerop write-count)
                   (zerop read-count))
          (incf write-count)
          (return-from lock-writer))))))

(defmethod lock-writer-try ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (with-spinlock (spinlock)
      (when (and (zerop write-count)
                 (zerop read-count))
        (incf write-count)))))

(defmethod lock-reader ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (loop
      (with-spinlock (spinlock)
        (when (zerop write-count)
          (incf read-count)
          (return-from lock-reader))))))

(defmethod lock-reader-try ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (with-spinlock (spinlock)
      (when (zerop write-count)
        (incf read-count)))))

(defmethod unlock ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (with-spinlock (spinlock)
      (if (plusp write-count)
          (decf write-count)
          (decf read-count)))))

(defmethod promote ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (with-spinlock (spinlock)
      (when (= read-count 1)
        (decf read-count)
        (incf write-count)))))

(defmethod demote ((lock spin-rw-lock))
  (with-slots (spinlock write-count read-count) lock
    (with-spinlock (spinlock)
      (decf write-count)
      (incf read-count))))

(defmacro with-spin-rw-lock ((lock writer) &body body)
  `(progn
     ,(case writer
        ((t)
           `(lock-writer ,lock))
        ((nil)
           `(lock-reader ,lock))
        (t
           `(if ,writer
                (lock-writer ,lock)
                (lock-reader ,lock))))
     (unwind-protect (progn ,@body)
       (unlock ,lock))))


(defclass slotted-spin-rw-lock ()
  ((locks_ :initarg :locks :initform nil)))

(defun make-slotted-spin-rw-lock (num)
  (make-instance 'slotted-spin-rw-lock
                 :locks (loop repeat num
                              collect (make-instance 'spin-rw-lock))))

(defmethod slotted-lock-writer ((lock slotted-spin-rw-lock) index)
  (with-slots (locks_) lock
    (lock-writer (nth index locks_))))

(defmethod slotted-lock-reader ((lock slotted-spin-rw-lock) index)
  (with-slots (locks_) lock
    (lock-reader (nth index locks_))))

(defmethod slotted-unlock ((lock slotted-spin-rw-lock) index)
  (with-slots (locks_) lock
    (unlock (nth index locks_))))

(defmethod slotted-lock-writer-all ((lock slotted-spin-rw-lock))
  (with-slots (locks_) lock
    (iterate ((x (scan locks_)))
             (lock-writer x))))

(defmethod slotted-lock-reader-all ((lock slotted-spin-rw-lock))
  (with-slots (locks_) lock
    (iterate ((x (scan locks_)))
             (lock-reader x))))

(defmethod slotted-unlock-all ((lock slotted-spin-rw-lock))
  (with-slots (locks_) lock
    (iterate ((x (scan locks_)))
             (unlock x))))


(defmacro with-slotted-rw-lock ((lock index writer) &body body)
  (alexandria:once-only (lock index)
    `(progn
       ,(case writer
          ((t)
             `(slotted-lock-writer ,lock ,index))
          ((nil)
             `(slotted-lock-reader ,lock, index))
          (t
             `(if ,writer
                  (slotted-lock-writer ,lock, index)
                  (slotted-lock-reader ,lock, index))))
       (unwind-protect (progn ,@body)
         (slotted-unlock ,lock ,index)))))


(defclass atomic-int ()
  ((%value :initarg :value :initform 0)
   (lock :initform (make-spinlock) :reader atomic-int-lock)))

(defmethod print-object ((self atomic-int) stream)
  (print-unreadable-object (self stream :type t :identity t)
    (format stream "~a" (slot-value self '%value))))

(defmethod atomic-int-value ((self atomic-int))
  (with-spinlock ((atomic-int-lock self))
    (slot-value self '%value)))

(defmethod (setf atomic-int-value) (value (self atomic-int))
  (with-spinlock ((atomic-int-lock self))
    (setf (slot-value self '%value) value)))

(defmethod atomic-int-add ((self atomic-int) value)
  (with-spinlock ((atomic-int-lock self))
    (prog1 (slot-value self '%value)
      (incf (slot-value self '%value) value))))

(defmethod atomic-int-cas ((self atomic-int) old-value new-value)
  (with-spinlock ((atomic-int-lock self))
    (when (= (slot-value self '%value) old-value)
      (setf (slot-value self '%value) new-value))))

(defmethod atomic-int-secure-least ((self atomic-int) val)
  (loop for cur = (atomic-int-value self)
        if (>= cur val)
          do (return cur)
        if (atomic-int-cas self cur val)
          do (return val)))
