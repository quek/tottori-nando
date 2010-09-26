(in-package :tottori-nando.internal)

(defun make-spinlock ()
  (cons nil nil))

(defun lock-spinlock (spinlock)
  (loop until (sb-ext:compare-and-swap (car spinlock) nil t)))

(defun unlock-spinlock (spinlock)
  (setf (car spinlock) nil))

(defmacro with-spinlock ((spinlock) &body body)
  (alexandria:once-only (spinlock)
    `(progn
       (lock-spinlock ,spinlock)
       (unwind-protect
            (progn ,@body)
         (unlock-spinlock ,spinlock)))))

(defclass spin-rw-lock ()
  ((spinlock :initform (make-spinlock))
   (wc :initform 0)
   (rc :initform 0)))


(defgeneric lock-writer (lock))
(defgeneric lock-writer-try (lock))
(defgeneric lock-reader (lock))
(defgeneric lock-reader-try (lock))
(defgeneric unlock (lock))
(defgeneric promote (lock))
(defgeneric demote (lock))

(defmethod lock-writer ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (loop
      (with-spinlock (spinlock)
        (when (and (<= wc 0)
                   (<= rc 0))
          (incf wc)
          (return-from lock-writer)))
      (sb-thread:thread-yield))))

(defmethod lock-writer-try ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (with-spinlock (spinlock)
      (when (and (<= wc 0)
                 (<= rc 0))
        (incf wc)))))

(defmethod lock-reader ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (loop
      (with-spinlock (spinlock)
        (when (<= wc 0)
          (incf rc)
          (return-from lock-reader)))
      (sb-thread:thread-yield))))

(defmethod lock-reader-try ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (with-spinlock (spinlock)
      (when (<= wc 0)
        (incf rc)))))

(defmethod unlock ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (with-spinlock (spinlock)
      (if (< 0 wc)
          (decf wc)
          (decf rc)))))

(defmethod promote ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (with-spinlock (spinlock)
      (when (<= rc 1)
        (decf rc)
        (incf wc)))))

(defmethod demote ((lock spin-rw-lock))
  (with-slots (spinlock wc rc) lock
    (with-spinlock (spinlock)
      (decf wc)
      (incf rc))))

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
  ((%value :initform 0)
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
