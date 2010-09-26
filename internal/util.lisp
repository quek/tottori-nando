(in-package :tottori-nando.internal)


;; (defun series-equal (a b &optional (test #'equal))
;;   (declare (optimizable-series-function))
;;   (collect-and (map-fn t test a b)))

(defun scan-byte (var size)
  (declare (optimizable-series-function))
  (scan-fn '(unsigned-byte 8)
           (lambda ()
             (logand (ash var (* (1- size) -8)) #xff))
           (lambda (_)
             (declare (ignore _))
             (decf size)
             (logand (ash var (* (1- size) -8)) #xff))
           (lambda (_)
             (declare (ignore _))
             (zerop size))))

(defun collect-byte (series)
  (declare (optimizable-series-function))
  (collect-fn 'integer (constantly 0)
              (lambda (acc x)
                (+ (ash acc 8) x))
              series))


(defun vector-push-byte-extend (vector var size)
  (iterate ((i (scan-byte var size)))
           (vector-push-extend i vector)))


(defun sym (&rest args)
  (intern (apply #'concatenate 'string
                 (mapcar (lambda (x) (string-upcase (string x))) args))))

(defmacro def-byte-struct (name &body slots)
  (alexandria:with-gensyms (stream buffer)
    (let ((total-size (loop for (a b slot-size array-size) in slots
                            sum (case slot-size
                                  (:array array-size)
                                  (:atomic-int 8)
                                  (t slot-size))))
          (idx -1))
      `(progn
         (defstruct ,name
           ,@(loop for (slot-name slot-init-val slot-size array-size) in slots
                   collect (list slot-name slot-init-val
                                 :type (case slot-size
                                         (:array t)
                                         (:atomic-int t)
                                         (t `(unsigned-byte ,(* 8 slot-size)))))))
         (defun ,(sym "READ-" name) (,name ,stream)
           (let ((,buffer (make-array ,total-size :element-type '(unsigned-byte 8))))
             (read-sequence ,buffer ,stream)
             ,@(loop for (slot-name slot-init-val slot-size array-size) in slots
                     with offset = 0
                     collect
                  (case slot-size
                    (:array
                       `(progn
                         ,@(loop for i from 0 below array-size
                                 collect `(setf (aref (,(sym name "-" slot-name) ,name) ,i)
                                                (aref ,buffer ,offset))
                                 do (incf offset))))
                    (:atomic-int
                       `(setf (atomic-int-value (,(sym name "-" slot-name) ,name))
                              (collect-byte (scan (subseq ,buffer ,offset
                                                          ,(incf offset 8))))))
                    (t
                       `(setf (,(sym name "-" slot-name) ,name)
                              (collect-byte (scan (subseq ,buffer ,offset
                                                          ,(incf offset slot-size))))))))))
         (defun ,(sym "WRITE-" name) (,name ,stream)
           (let ((,buffer (make-array ,total-size :element-type '(unsigned-byte 8))))
             ,@(loop for (slot-name slot-init-val slot-size array-size) in slots
                     collect
                  (case slot-size
                    (:array
                       `(progn
                          ,@(loop for i from 0 below array-size
                                  collect `(setf (aref ,buffer ,(incf idx))
                                                 (aref (,(sym name "-" slot-name) ,name) ,i)))))
                    (:atomic-int
                       `(let ((val (atomic-int-value (,(sym name "-" slot-name) ,name))))
                          (setf ,@(loop for i from 56 downto 0 by 8
                                        append `((aref ,buffer ,(incf idx))
                                                 (ldb (byte 8  ,i) val))))))
                    (t
                       `(let ((val (,(sym name "-" slot-name) ,name)))
                          (setf ,@(loop for i from (* 8 (1- slot-size)) downto 0 by 8
                                        append `((aref ,buffer ,(incf idx))
                                                 (ldb (byte 8  ,i) val))))))))
             (write-sequence ,buffer ,stream)))))))


(defmacro defun-write-var-num ()
  `(defun write-var-num (buf num &optional (start 0))
     (cond
       ((< num (ash 1 7))
        (setf (aref buf start) num)
        1)
       ,@(loop for i from 2 to 10 collect
               `(,(if (= i 10)
                      t
                      `(< num (ash ,i ,(* i 7))))
                      (setf
                        ,@(loop for j from 0 below i append
                          (cond
                            ((= j 0)
                             `((aref buf (+ ,j start))
                               (logior (ash num ,(* (1- i) -7)) #x80)))
                            ((= j (1- i))
                             `((aref buf (+ ,j start))
                               (logand num #x7f)))
                            (t
                             `((aref buf (+ ,j start))
                               (logior (logand (ash num ,(* (- i j 1) -7)) #x7f) #x80))))))
                      ,i)))))
(defun-write-var-num)

(defun read-var-num (buf size &optional (start 0))
  (loop repeat size
        for rp from start
        for c = (aref buf rp)
        for num = (logand c #x7f) then (+ (ash num 7) (logand c #x7f))
        while (<= #x80 c)
        finally (return (values (- rp start -1) num))))

(declaim (inline write-fixnum))
(defun write-fixnum (buf num width &optional (start 0) )
  (iterate ((v (scan-byte num width))
            (i (scan-range)))
           (setf (aref buf (+ i start)) v)))

(declaim (inline read-fixnum))
(defun read-fixnum (buffer width &optional (start 0))
  (collect-byte (subseries (scan buffer) start (+ start width))))

(defmacro n++ (var &optional (delta 1))
  `(prog1 ,var (incf ,var ,delta)))


(defun string-to-octets (string)
  (sb-ext:string-to-octets string :external-format :utf-8))

(defun octets-to-string (octets &key end)
  (sb-ext:octets-to-string octets :external-format :utf-8 :end end))


(declaim (inline hash-murmur))
(defun hash-murmur (buf size)
  (declare (type (simple-array (unsigned-byte 8) (*)) buf)
           (type (unsigned-byte 64) size)
           (optimize speed (safety 0))
           (values (unsigned-byte 64)))
  (let* ((mul #xc6a4a7935bd1e995)
         (rtt 47)
         (-rtt (- rtt))
         (hash (the (unsigned-byte 64) (logxor 19780211 (the (unsigned-byte 64) (* size mul)))))
         (i 0))
    (declare (type (unsigned-byte 64) hash)
             (type fixnum i))
    (loop while (<= 8 size)
          do (let ((num (logior (aref buf i)
                                (ash (aref buf (1+ i)) 8)
                                (ash (aref buf (+ i 2)) 16)
                                (ash (aref buf (+ i 3)) 24)
                                (ash (aref buf (+ i 4)) 32)
                                (ash (aref buf (+ i 5)) 40)
                                (ash (aref buf (+ i 6)) 48)
                                (ash (aref buf (+ i 7)) 56))))
               (declare (type (unsigned-byte 64) num))
               (setf num (* num mul))
               (setf num (logxor num (ash num -rtt)))
               (setf num (* num mul))
               (setf hash (logxor hash num))
               (setf hash (* hash mul))
               (incf i 8)
               (decf size 8)))
    (tagbody
       (case size
         (6 (go 6))
         (5 (go 5))
         (4 (go 4))
         (3 (go 3))
         (2 (go 2))
         (1 (go 1))
         (0 (go 0)))
       (setf hash (logxor hash (ash (aref buf (+ i 6)) 48)))
     6 (setf hash (logxor hash (ash (aref buf (+ i 5)) 40)))
     5 (setf hash (logxor hash (ash (aref buf (+ i 4)) 32)))
     4 (setf hash (logxor hash (ash (aref buf (+ i 3)) 24)))
     3 (setf hash (logxor hash (ash (aref buf (+ i 2)) 16)))
     2 (setf hash (logxor hash (ash (aref buf (1+ i)) 8)))
     1 (setf hash (logxor hash (aref buf i)))
       (setf hash (* hash mul))
     0)
    (setf hash (logxor hash (ash hash -rtt)))
    (setf hash (* hash mul))
    (setf hash (logxor hash (ash hash -rtt)))
    hash))

(defun hash-murmur-fixnum-version-たぶん間違ってる (buf size)
  (declare (type (simple-array (unsigned-byte 8) (*)) buf)
           (type fixnum size)
           (optimize speed (safety 0))
           (values fixnum))
  (let* ((mul #x6a4a7935bd1e995)
         (rtt 46)
         (-rtt (- rtt))
         (hash (the fixnum (logxor 19730426 (the fixnum (* (the (unsigned-byte 64) size) mul)))))
         (i 0))
    (declare (type fixnum hash i))
    (loop while (<= 7 size)
          do (let ((num (logior (aref buf i)
                                (ash (aref buf (1+ i)) 8)
                                (ash (aref buf (+ i 2)) 16)
                                (ash (aref buf (+ i 3)) 24)
                                (ash (aref buf (+ i 4)) 32)
                                (ash (aref buf (+ i 5)) 40)
                                (ash (aref buf (+ i 6)) 48))))
               (declare (type fixnum num))
               (setf num (* num mul))
               (setf num (logxor num (ash num -rtt)))
               (setf num (* num mul))
               (setf hash (logxor hash num))
               (setf hash (the fixnum (* (the (unsigned-byte 64) hash)
                                         (the (unsigned-byte 64) mul))))
               (incf i 8)
               (decf size 8)))
    (tagbody
       (case size
         (5 (go 5))
         (4 (go 4))
         (3 (go 3))
         (2 (go 2))
         (1 (go 1))
         (0 (go 0)))
       (setf hash (logxor hash (ash (aref buf (+ i 5)) 40)))
     5 (setf hash (logxor hash (ash (aref buf (+ i 4)) 32)))
     4 (setf hash (logxor hash (ash (aref buf (+ i 3)) 24)))
     3 (setf hash (logxor hash (ash (aref buf (+ i 2)) 16)))
     2 (setf hash (logxor hash (ash (aref buf (1+ i)) 8)))
     1 (setf hash (logxor hash (aref buf i)))
       (setf hash (* (the (unsigned-byte 64) hash) (the (unsigned-byte 64) mul)))
     0)
    (setf hash (logxor hash (ash hash -rtt)))
    (setf hash (* (the (unsigned-byte 64) hash) (the (unsigned-byte 64) mul)))
    (setf hash (logxor hash (ash hash -rtt)))
    hash))

(defun hash-murmur-str (str)
  (let ((buf (sb-ext:string-to-octets str :external-format :utf-8)))
    (hash-murmur buf (length buf))))
