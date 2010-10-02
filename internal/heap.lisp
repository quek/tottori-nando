(in-package :tottori-nando.internal)

(defstruct heap
  (size 0 :type fixnum)
  (fragments-offset 0 :type fixnum)
  (fragments nil)
  (file))

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
  (with-slots (file) heap
    (let ((offset (heap-size heap))
          (whole-size (+ size 8 (padding size))))
      (setf (ref-64 (db-stream-sap file) (heap-size heap)) whole-size)
      (incf (heap-size heap) whole-size)
      (make-fragment :offset (+ offset 8) :size (- size 8)))))

(defun alloc (heap size)
  (loop for x in (heap-fragments heap)
        and prev = nil then x
        if (<= size (fragment-size x))
          do (if prev
                 (setf (fragment-next prev) (fragment-next x))
                 (setf (heap-fragments heap) (fragment-next x)))
             (return-from alloc (fragment-offset x)))
  (fragment-offset (%alloc heap size)))

(defun free (heap offset)
  (with-slots (file) heap
    (let ((size (ref-64 (db-stream-sap file) (- offset 8))))
      (loop with new-fragment = (make-fragment :offset offset :size size)
            for x in (heap-fragments heap)
            and prev = nil then x
            if (< (fragment-size x) size)
              do (if prev
                     (shiftf (fragment-next new-fragment) (fragment-next prev) new-fragment)
                     (shiftf (fragment-size new-fragment) (heap-fragments heap) new-fragment))))))

(defun dump-fragments (heap)
  (let* ((sap (db-stream-sap (heap-file heap)))
         (heap-fragments (heap-fragments heap)))
    (setf (ref-64 sap (heap-fragments-offset heap))
          (if heap-fragments
              (fragment-offset (heap-fragments heap))
              0))
    (loop for i = heap-fragments then (fragment-next i)
          while i
          for j = (fragment-next i)
          for next-offset = (if j (fragment-offset j) 0)
          do (setf (ref-64 sap (fragment-offset i))
                   next-offset))
    heap))

(defun load-fragments (heap)
  (let* ((sap (db-stream-sap (heap-file heap)))
         (start (ref-64 sap (heap-fragments-offset heap))))
    (let ((fragments (loop for offset = start then next
                           until (zerop offset)
                           for next = (ref-64 sap offset)
                           collect (make-fragment :offset offset
                                        :size (ref-64 sap (- offset 8))))))
      (setf (heap-fragments heap) (car fragments))
      (loop for i in fragments
            and j in (cdr fragments)
            do (setf (fragment-next i) j)))
    heap))
