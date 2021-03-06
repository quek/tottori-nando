(in-package :tottori-nando.internal)

(defstruct heap
  (start 0 :type fixnum)
  (end 0 :type fixnum)
  (fragments-offset 0 :type fixnum)
  (fragments nil)
  (stream)
  (lock (make-spinlock)))

(defstruct fragment
  (offset 0 :type fixnum)
  (size 0 :type fixnum)
  (next nil :type (or fragment null)))

(defun padding (size)
  (let ((x (logand size 7)))
    (if (zerop x)
        0
        (- 8 x))))

(defun %alloc (heap size)
  (with-slots (stream) heap
    (let ((offset (heap-end heap))
          (whole-size (+ size 8 (padding size)))) ; 8 はサイズの分
      (write-64-at stream (heap-end heap) whole-size)
      (incf (heap-end heap) whole-size)
      (make-fragment :offset (+ offset 8) :size (- whole-size 8)))))

(defun alloc (heap size)
  (with-spinlock ((heap-lock heap))
    (loop for x = (heap-fragments heap) then (fragment-next x)
          and prev = nil then x
          while x
          if (<= size (fragment-size x))
            do (if prev
                   (setf (fragment-next prev) (fragment-next x))
                   (setf (heap-fragments heap) (fragment-next x)))
               (return-from alloc (fragment-offset x)))
    (values (fragment-offset (%alloc heap size))
            heap)))

(defun free (heap offset)
  (with-spinlock ((heap-lock heap))
    (with-slots (stream) heap
      (let ((size (- (read-64-at stream (- offset 8)) 8)))
        (loop with new-fragment = (make-fragment :offset offset :size size)
              for x = (heap-fragments heap) then (fragment-next x)
              and prev = nil then x
              unless x
                do (if prev
                       (setf (fragment-next prev) new-fragment)
                       (setf (heap-fragments heap) new-fragment))
                   (return)
              if (< size (fragment-size x))
                do (if prev
                       (shiftf (fragment-next new-fragment) (fragment-next prev) new-fragment)
                       (shiftf (fragment-next new-fragment) (heap-fragments heap) new-fragment))
                   (return))))
    heap))

(defun dump-fragments (heap)
  (with-spinlock ((heap-lock heap))
    (let* ((stream (heap-stream heap))
           (heap-fragments (heap-fragments heap)))
      (setf (heap-fragments-offset heap)
            (if heap-fragments
                (fragment-offset heap-fragments)
                0))
      (loop for i = heap-fragments then (fragment-next i)
            while i
            for j = (fragment-next i)
            for next-offset = (if j (fragment-offset j) 0)
            do (write-64-at stream (fragment-offset i) next-offset))
      heap)))

(defun load-fragments (heap)
  (with-spinlock ((heap-lock heap))
    (let ((heap-fragments-offset (heap-fragments-offset heap))
          (stream (heap-stream heap)))
      (unless (zerop heap-fragments-offset)
        (let ((fragments (loop for offset = heap-fragments-offset then (read-64-at stream offset)
                               until (zerop offset)
                               collect (make-fragment :offset offset
                                            :size (- (read-8-at stream (- offset 8)) 8)))))
          (setf (heap-fragments heap) (car fragments))
          (loop for i in fragments
                and j in (cdr fragments)
                do (setf (fragment-next i) j)))))
    heap))
