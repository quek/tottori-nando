(in-package :tottori-nando)

(alexandria:define-constant +hdbmagicdata+
    (string-to-octets (format nil "KC~c~c" #\Newline #\Nul))
  :test #'equalp
  :documentation "magic data of the file")
(alexandria:define-constant +hdbchksumseed+ (sb-ext:string-to-octets "__kyotocabinet__")
  :test #'equalp
  :documentation "seed of the module checksum")
;; (defconstant +hdbmofflibver+ 4 "offset of the library version")
;; (defconstant +hdbmofflibrev+ 5 "offset of the library revision")
;; (defconstant +hdbmofffmtver+ 6 "offset of the format revision")
;; (defconstant +hdbmoffchksum+ 7 "offset of the module checksum")
;; (defconstant +hdbmofftype+ 8 "offset of the database type")
;; (defconstant +hdbmoffapow+ 9 "offset of the alignment power")
;; (defconstant +hdbmofffpow+ 10 "offset of the free block pool power")
;; (defconstant +hdbmoffopts+ 11 "offset of the options")
;; (defconstant +hdbmoffbnum+ 16 "offset of the bucket number")
;; (defconstant +hdbmoffflags+ 24 "offset of the status flags")
;; (defconstant +hdbmoffcount+ 32 "offset of the record number")
;; (defconstant +hdbmoffsize+ 40 "offset of the file size")
(defconstant +hdbmoffopaque+ 48 "offset of the opaque data")
(defconstant +hdbheadsiz+ 64 "size of the header")
(defconstant +hdbfbpwidth+ 6 "width of the free block")
(defconstant +hdbwidthlarge+ 6 "large width of the record address")
(defconstant +hdbwidthsmall+ 4 "small width of the record address")
(defconstant +hdbrecbufsiz+ 48 "size of the record buffer")
(defconstant +hdbiobufsiz+ 1024 "size of the io buffer")
(defconstant +hdbrlockslot+ 64 "number of slots of the record lock")
(defconstant +hdbdefapow+ 3 "default alignment power")
(defconstant +hdbmaxapow+ 15 "maximum alignment power")
(defconstant +hdbdeffpow+ 10 "default free block pool power")
(defconstant +hdbmaxfpow+ 20 "maximum free block pool power")
(defconstant +hdbdefbnum+ 1048583 "default bucket number")
(defconstant +hdbdefmsiz+ (ash 64 20) "default size of the memory-mapped region")
(defconstant +hdbrecmagic+ #xcc "magic data for record")
(defconstant +hdbpadmagic+ #xee "magic data for padding")
(defconstant +hdbfbmagic+ #xdd "magic data for free block")
(defconstant +hdbdfrgmax+ 512 "maximum unit of auto defragmentation")
(defconstant +hdbdfrgcef+ 2 "coefficient of auto defragmentation")
(alexandria:define-constant +hdbtmppathext+ "tmpkch"
  :test #'string=
  :documentation "extension of the temporary file")


(def-byte-struct head
  (magic (copy-seq +hdbmagicdata+) :array #.(length +hdbmagicdata+))
  (libver +libver+ 1)
  (librev +librev+ 1)
  (fmtver +formatver+ 1)
  (chksum 0 1)
  (type +typehash+ 1)
  (apow +hdbdefapow+ 1)
  (fpow +hdbdeffpow+ 1)
  (opts 0 1)
  (dummy1 0 4)
  (bnum +hdbdefbnum+ 8)
  (flags 0 1)
  (dummy2 0 7)
  (count (make-instance 'atomic-int) :atomic-int)
  (lsiz (make-instance 'atomic-int) :atomic-int)
  (opaque (make-array #1=#.(- +hdbheadsiz+ +hdbmoffopaque+)
                      :element-type '(unsigned-byte 8))
          :array #1#))


(defclass hash-db (basic-db)
  ((mlock_ :initform (make-instance 'spin-rw-lock) :documentation "The method lock.")
   (rlock_ :initform (make-slotted-spin-rw-lock +hdbrlockslot+)  :documentation "The record locks.")
   (flock_ :initform (make-spinlock) :documentation "The file lock.")
   (atlock_ :initform (sb-thread::make-mutex) :documentation "The auto transaction lock.")
   (error_ :initform nil :documentation "The last happened error.")
   (logger_ :initform nil :documentation "The internal logger.")
   (logkinds_ :initform 0 :documentation "The kinds of logged messages.")
   (omode_ :initform 0 :documentation "The open mode.")
   (writer_ :initform nil :documentation "The flag for writer.")
   (autotran_ :initform nil :documentation "The flag for auto transaction.")
   (autosync_ :initform nil :documentation "The flag for auto synchronization.")
   (reorg_ :initform nil :documentation "The flag for reorganized.")
   (trim_ :initform nil :documentation "The flag for trimmed.")
   (file_ :initform nil :documentation "The file for data.")
   (fbp_ :initform nil :documentation "The free block pool.")
   (curs_ :initform nil :documentation "The cursor objects.")
   (path_ :initform nil :documentation "The path of the database file.")
   (head_ :initform (make-head) :documentation "header")
   ;;(libver_ :initform 0 :type (unsigned-byte 8) :documentation "The library version.")
   ;;(librev_ :initform 0 :type (unsigned-byte 8) :documentation "The library revision.")
   ;;(fmtver_ :documentation "The format revision.")
   ;;(chksum_ :documentation "The module checksum.")
   ;;(type_ :documentation "The database type.")
   ;;(apow_ :initform +hdbdefapow+ :documentation "The alignment power.")
   ;;(fpow_ :initform +hdbdeffpow+ :documentation "The free block pool power.")
   ;;(opts_ :documentation "The options.")
   ;;(bnum_ :initform +hdbdefbnum+ :documentation "The bucket number.")
   ;;(flags_ :documentation "The status flags.")
   (flagopen_ :initform nil :documentation "The flag for open.")
   ;;(count_ :documentation "The record number.")
   ;;(lsiz_ :documentation "The logical size of the file.")
   (psiz_ :initform (make-instance 'atomic-int) :documentation "The physical size of the file.")
   ;;(opaque_ :documentation "The opaque data.")
   (msiz_ :initform +hdbdefmsiz+ :type fixnum :documentation "The size of the internal memory-mapped region.")
   (dfunit_ :initform 0 :type fixnum :documentation "The unit step number of auto defragmentation.")
   (embcomp_ :initform nil :documentation "The embedded data compressor.")
   (align_ :initform 0 :type fixnum :documentation "The alignment of records.")
   (fbpnum_ :initform 0 :type fixnum :documentation "The number of elements of the free block pool.")
   (width_ :initform 0 :type fixnum :documentation "The width of record addressing.")
   (linear_ :initform nil :documentation "The flag for linear collision chaining.")
   (comp_ :initform nil :documentation "The data compressor.")
   (rhsiz_ :initform 0 :type fixnum :documentation "The header size of a record.")
   (boff_ :initform 0 :type fixnum :documentation "The offset of the buckets section.")
   (roff_ :initform 0 :type fixnum :documentation "The offset of the record section.")
   (dfcur_ :initform 0 :documentation "The defrag cursor.")
   (frgcnt_ :initform (make-instance 'atomic-int) :documentation "The count of fragmentation.")
   (tran_ :initform nil :documentation "The flag whether in transaction.")
   (trhard_ :initform nil :documentation "The flag whether hard transaction.")
   (trfbp_ :initform nil :documentation "The escaped free block pool for transaction.")))

(defmethod print-object ((db hash-db) stream)
  (print-unreadable-object (db stream)
    (format stream "~a"
            (loop for i in '(head_ psiz_ msiz_ dfunit_ fbpnum_ rhsiz_ boff_
                             roff_ dfcur_ frgcnt_ trfbp_)
                  collect (cons i (slot-value db i))))))


(defmethod hash-record ((db hash-db) buffer size)
  (hash-murmur buffer size))

(defmethod fold-hash ((db hash-db) hash)
  (declare (type (unsigned-byte 64) hash))
  (logxor (logior (ash (logand hash #xffff000000000000) -48)
                  (ash (logand hash #x0000ffff00000000) -16))
          (logior (ash (logand hash #x000000000000ffff) 16)
                  (ash (logand hash #x00000000ffff0000) -116))))


(defmethod compare-keys ((db hash-db) abuf asiz bbuf bsiz)
  (declare (type (simple-array (unsigned-byte 8) (*)) abuf bbuf)
           (type fixnum asiz bsiz))
  (if (/= asiz bsiz)
      (- asiz bsiz)
      (and (every #'= abuf bbuf) 0)))

(defmethod set-bucket ((db hash-db) bidx off)
  (with-slots (file_ width_ boff_ head_) db
    (let ((buf (make-array width_ :element-type '(unsigned-byte 8))))
      (write-fixnum buf (ash off (- (head-apow head_))) width_)
      (file-position file_ (+ boff_ (* bidx width_)))
      (write-sequence buf file_))))

(defmethod get-backet ((db hash-db) bucket-index)
  (with-slots (boff_ width_ file_ head_) db
    (let ((buffer (make-array width_ :element-type '(unsigned-byte 8))))
      (file-position file_ (+ boff_ (* bucket-index width_)))
      (read-sequence buffer file_)
      (ash (read-fixnum buffer width_) (head-apow head_)))))

(defconstant +tsmall+ (ash 1 0) "use 32-bit addressing")
(defconstant +tlinear+ (ash 1 1) "use linear collision chaining")
(defconstant +tcompress+ (ash 1 2) "compress each record")

(defmethod calc-meta ((db hash-db))
  (with-slots (align_ head_ fbpnum_ width_ linear_ rhsiz_ boff_
                      roff_ dfcur_ frgcnt_ comp_ embcomp_) db
    (setf align_ (ash 1 (head-apow head_))
          fbpnum_ (if (> (head-fpow head_) 0) (ash 1 (head-fpow head_)) 0)
          width_ (if (< 0 (logand +tsmall+ (head-opts head_))) +hdbwidthsmall+ +hdbwidthlarge+)
          linear_ (not (null (< 0 (logand +tlinear+  (head-opts head_)))))
          comp_ (if (< 0 (logand +tcompress+  (head-opts head_))) embcomp_)
          rhsiz_ (+ 2 (* 1 2))
          rhsiz_ (+ rhsiz_ (if linear_ width_ (* width_ 2)))
          boff_ (+ +hdbheadsiz+ (* +hdbfbpwidth+ fbpnum_))
          boff_ (if (< 0 fbpnum_) (+ boff_ (* width_  2) (* 1 2)) boff_)
          roff_ (+ boff_ (* width_ (head-bnum head_)))
          roff_ (let ((rem (mod roff_ align_)))
                  (if (< 0 rem) (+ roff_ align_ (- rem)) roff_))
          dfcur_ roff_
          frgcnt_ 0)))

(defmethod calc-checksum ((db hash-db))
  0)


(defmethod dump-meta ((db hash-db))
  (with-slots (head_ file_) db
    (file-position file_ 0)
    (write-head head_ file_)))

(defmethod load-meta ((db hash-db))
  (with-slots (head_ psiz_ file_) db
    (file-position file_ 0)
    (read-head head_ file_)
    (setf (atomic-int-value psiz_) (atomic-int-value (head-lsiz head_)))))

(defstruct record
  (off)
  (rsiz)
  (psiz)
  (ksiz)
  (vsiz)
  (left)
  (right)
  (kbuf)
  (vbuf)
  (boff)
  (bbuf))

(defmethod calc-record-padding ((db hash-db) rsiz)
  (with-slots (align_) db
    (let ((diff (logand rsiz (1- align_ ))))
      (if (> diff 0)
          (- align_ diff)
          0))))

(defmethod read-record ((db hash-db) record buffer)
  (with-slots (head_ file_ linear_ roff_ psiz_ rhsiz_ width_) db
    (let ((rsiz (if (< +hdbrecbufsiz+ (- (atomic-int-value psiz_) (record-off record)))
                    +hdbrecbufsiz+
                    rhsiz_)))
      (file-position file_ (record-off record))
      (read-sequence buffer file_ :end rsiz)
      (let ((rp 0)
            snum)
        (cond ((= (aref buffer rp) +hdbrecmagic+)
               (setf snum (aref buffer (1+ rp))))
              ((>= (aref buffer rp) #x80)
               (when (or (/= (aref buffer (n++ rp)) +hdbfbmagic+)
                         (/= (aref buffer (n++ rp)) +hdbfbmagic+))
                 (error "invalid magic data of a free block"))
               (setf (record-rsiz record)
                     (ash (read-fixnum buffer width_ rp) (head-apow head_)))
               (incf rp width_)
               (when (or (/= (aref buffer (n++ rp)) +hdbpadmagic+)
                         (/= (aref buffer (n++ rp)) +hdbpadmagic+))
                 (error "invalid magic data of a free block"))
               (setf (record-psiz record) #xffff
                     (record-ksiz record) 0
                     (record-vsiz record) 0
                     (record-left record) 0
                     (record-right record) 0
                     (record-kbuf record) nil
                     (record-vbuf record) nil
                     (record-boff record) 0
                     (record-bbuf record) nil)
               (return-from read-record t))
              ((zerop (aref buffer rp))
               (error "nullified region"))
              (t
               (setf snum (collect-byte (subseries (scan 'vector buffer) rp 2)))))
        (incf rp 2)
        (decf rsiz 2)
        (setf (record-psiz record) snum
              (record-left record) (ash (read-fixnum buffer width_ rp) (head-apow head_)))
        (incf rp width_)
        (decf rsiz width_)
        (if linear_
            (setf (record-right record) 0)
            (progn
              (setf (record-right record)
                    (ash (read-fixnum buffer width_ rp) (head-apow head_)))
              (incf rp width_)
              (decf rsiz width_)))
        (multiple-value-bind (step num) (read-var-num buffer rsiz rp)
          (when (< step 1)
            (error "invalid key length"))
          (setf (record-ksiz record) num)
          (incf rp step)
          (decf rsiz step)
          (setf (values step num) (read-var-num buffer rsiz rp))
          (when (< step 1)
            (error "invalid value length"))
          (setf (record-vsiz record) num)
          (incf rp step)
          (decf rsiz step)
          (let ((hsiz rp))
            (setf (record-rsiz record) (+ hsiz
                                          (record-ksiz record)
                                          (record-vsiz record)
                                          (record-psiz record))
                  (record-kbuf record) nil
                  (record-kbuf record) nil
                  (record-boff record) (+ (record-off record) hsiz)
                  (record-bbuf record) nil))
          (if (>= rsiz (record-ksiz record))
              (progn
                (setf (record-kbuf record) (subseq buffer rp (+ rp (record-ksiz record))))
                (incf rp (record-ksiz record))
                (decf rsiz (record-ksiz record))
                (when (>= rsiz (record-vsiz record))
                  (setf (record-vbuf record) (subseq buffer rp (+ rp (record-vsiz record))))
                  (when (> (record-psiz record) 0)
                    (incf rp (record-vsiz record))
                    (decf rsiz (record-vsiz record))
                    (when (and (> rsiz 0) (/= (aref buffer rp) +hdbpadmagic+))
                      (error "invalid magic data of a record. record=~a buffer=~a rp=~a rsiz=~a"
                             record buffer rp rsiz)))))
              (read-record-body db record)))))))

(defmethod read-record-body ((db hash-db) record)
  (with-slots (file_) db
    (let ((bsiz (+ (record-ksiz record) (record-vsiz record))))
      (when (> (record-psiz record) 0)
        (incf bsiz))
      (let ((bbuf (make-array bsiz :element-type '(unsigned-byte 8))))
        (file-position file_ (record-boff record))
        (read-sequence bbuf file_)
        (when (and (> (record-psiz record) 0) (/= (aref bbuf (1- bsiz)) +hdbpadmagic+))
          (error "invalid magic data of a record. data=~a" bbuf))
        (setf (record-bbuf record) bbuf
              (record-kbuf record) (subseq (record-bbuf record) 0 (record-ksiz record))
              (record-vbuf record) (subseq (record-bbuf record)
                                           (record-ksiz record)
                                           (+ (record-ksiz record) (record-vsiz record))))
        record))))

(defmethod write-record ((db hash-db) rec over)
  (with-slots (head_ linear_ file_ width_) db
    (let* ((rbuf (make-array (record-rsiz rec) :element-type '(unsigned-byte 8)))
           (wp 0)
           (snum (record-psiz rec)))
      (setf (aref rbuf 0) (logand (ash snum -8) #xff))
      (setf (aref rbuf 1) (logand snum #xff))
      (when (< (record-psiz rec) #x100)
        (setf (aref rbuf wp) +hdbrecmagic+))
      (incf wp 2)
      (write-fixnum rbuf (ash (record-left rec) (- (head-apow head_))) width_ wp)
      (incf wp width_)
      (unless linear_
        (write-fixnum rbuf (ash (record-right rec) (- (head-apow head_))) width_ wp)
        (incf wp width_))
      (incf wp (write-var-num rbuf (record-ksiz rec) wp))
      (incf wp (write-var-num rbuf (record-vsiz rec) wp))
      (iterate ((x (scan (record-kbuf rec)))
                (i (scan-range :from wp :length (record-ksiz rec))))
        (setf (aref rbuf i) x))
      (incf wp (record-ksiz rec))
      (iterate ((x (scan (record-vbuf rec)))
                (i (scan-range :from wp :length (record-vsiz rec))))
        (setf (aref rbuf i) x))
      (incf wp (record-vsiz rec))
      (when (> (record-psiz rec) 0)
        (iterate ((i (scan-range :from wp :length (record-psiz rec))))
          (setf (aref rbuf i) 0))
        (setf (aref rbuf wp) +hdbpadmagic+)
        (incf wp (record-psiz rec)))
      (if over
          (progn
            (file-position file_ (record-off rec))
            ;; TODO write_fast
            (write-sequence rbuf file_))
          (progn
            (file-position file_ (record-off rec))
            (write-sequence rbuf file_))))))

(defmethod adjust-record ((db hash-db) rec)
  (with-slots (head_ rhsiz_ file_) db
    (when (or (> (record-psiz rec) #xffff) (> (record-psiz rec) (/ (record-rsiz rec))))
      (let ((nsiz (ash (ash (record-psiz rec) (- (head-apow head_))) (head-apow head_))))
        (when (< nsiz rhsiz_)
          (return-from adjust-record t))
        (decf (record-rsiz rec) nsiz)
        (decf (record-psiz rec) nsiz)
        (let ((noff (+ (record-off rec) (record-rsiz rec)))
              (nbuf (make-array +hdbrecbufsiz+ :element-type '(unsigned-byte 8))))
          (file-position file_ noff)
          (write-sequence nbuf file_)))))
  t)

(defmethod calc-record-size ((db hash-db) ksiz vsiz)
  (with-slots (width_ linear_) db
    (let ((rsiz (+ 2 width_)))
      (unless linear_
        (incf rsiz width_))
      (cond ((< ksiz (ash 1 7))
             (incf rsiz))
            ((< ksiz (ash 1 14))
             (incf rsiz 2))
            ((< ksiz (ash 1 21))
             (incf rsiz 3))
            ((< ksiz (ash 1 28))
             (incf rsiz 4))
            (t
             (incf rsiz 5)))
      (cond ((< vsiz (ash 1 7))
             (incf rsiz))
            ((< vsiz (ash 1 14))
             (incf rsiz 2))
            ((< vsiz (ash 1 21))
             (incf rsiz 3))
            ((< vsiz (ash 1 28))
             (incf rsiz 4))
            (t
             (incf rsiz 5)))
      (incf rsiz ksiz)
      (incf rsiz vsiz)
      rsiz)))

(defmethod cut-chain ((db hash-db) rec rbuf bidx entoff)
  (with-slots (width_) db
    (let ((child (cond ((and (> (record-left rec) 0)
                             (< (record-right rec) 1))
                        (record-left rec))
                       ((and (< (record-left rec) 1)
                             (> (record-right rec) 0))
                        (record-right rec))
                       ((< (record-left rec) 1)
                        0)
                       (t
                        (let ((prec (make-record :off (record-left rec))))
                          (read-record db prec rbuf)
                          (when (= (record-psiz prec) #xffff)
                            (error "free block in the chain"))
                          (if (> (record-rsiz prec) 0)
                              (let ((off (record-rsiz prec))
                                    (pentoff (+ (record-off prec) 2 width_)))
                                (loop
                                  (setf (record-off prec) off)
                                  (read-record db prec rbuf)
                                  (when (= (record-psiz prec) #xffff)
                                    (error "free block in the chain"))
                                  (when (< (record-right prec) 1)
                                    (return))
                                  (setf off (record-rsiz prec)
                                        pentoff (+ (record-off prec) 2 width_)))
                                (set-chain db pentoff (record-left prec))
                                (set-chain db (+ off 2) (record-left rec))
                                (set-chain db (+ off 2 width_) (record-right rec))
                                off)
                              (progn
                                (set-chain db (+ (record-off prec) 2 width_) (record-right rec))
                                (record-off prec))))))))
      (if (> entoff 0)
          (set-chain db entoff child)
          (set-bucket db bidx child)))))

(defmethod set-chain ((db hash-db) entoff off)
  (with-slots (file_ head_ width_) db
    (let ((buf (make-array width_ :element-type '(unsigned-byte 8))))
      (write-fixnum buf (ash off (- (head-apow head_))) width_)
      (file-position file_ entoff)
      (write-sequence buf file_))))


(defstruct free-block
  (off 0)
  (rsiz 0))

(defun free-block< (a b)
  (or (< (free-block-rsiz a)
         (free-block-rsiz b))
      (and (= (free-block-rsiz a)
              (free-block-rsiz b))
           (> (free-block-off a)
              (free-block-off b)))))

(defmethod load-free-blocks ((db hash-db))
  (with-slots (fbpnum_ boff_ file_ head_ fbp_) db
    (when (< fbpnum_ 1)
      (return-from load-free-blocks t))
    (let* ((size (- boff_ +hdbheadsiz+))
           (rbuf (make-array size :element-type '(unsigned-byte 8))))
      (read-sequence rbuf file_)
      (let ((blocks (loop with num = 0
                          with rp = 0
                          while (and (< num fbpnum_) (< 1 size) (/= 0 (aref rbuf rp)))
                          collect (multiple-value-bind (step off)
                                      (read-var-num rbuf size rp)
                                    (when (or (< step 1) (< off 1))
                                      (error "invalid free block offset. step=~d, off=~d"
                                             step off))
                                    (incf rp step)
                                    (decf size step)
                                    (multiple-value-bind (step rsiz)
                                        (read-var-num rbuf size rp)
                                      (when (or (< step 1) (< rsiz 1))
                                        (error "invalid free block offset. step=~d, rsiz=~d"
                                               step off))
                                      (incf rp step)
                                      (decf size step)
                                      (make-free-block :off (ash off (head-apow head_))
                                                       :rsiz (ash rsiz (head-apow head_))))))))
        (loop for prev = (car blocks) then i
              for i in (cdr blocks)
              do (incf (free-block-off i)
                       (free-block-off prev)))
        (setf fbp_ (sort (append fbp_ blocks) #'free-block<))))))

(defmethod write-free-block ((db hash-db) off rsiz rbuf)
  (with-slots (head_ file_ width_) db
    (let ((wp 0))
      (setf (aref rbuf (n++ wp)) +hdbfbmagic+
            (aref rbuf (n++ wp)) +hdbfbmagic+)
      (write-fixnum rbuf (ash rsiz (- (head-apow head_))) width_ wp)
      (incf wp width_)
      (setf (aref rbuf (n++ wp)) +hdbpadmagic+
            (aref rbuf (n++ wp)) +hdbpadmagic+)
      (file-position file_ off)
      (write-sequence rbuf file_ :end wp))))

(defmethod insert-free-block ((db hash-db) off rsiz)
  (with-slots (flock_ fbp_ fbpnum_) db
    (with-spinlock (flock_)
      ;; TODO (escape-cursors off (+ off rsiz))
      (when (< fbpnum_ 1)
        (return-from insert-free-block))
      (when (>= (length fbp_) fbpnum_)
        (when (<= rsiz (free-block-rsiz (car fbp_)))
          (return-from insert-free-block))
        (setf fbp_ (cdr fbp_)))
      (push (make-free-block :off off :rsiz rsiz) fbp_)
      (setf fbp_ (sort fbp_ #'free-block<)))))

(defmethod fetch-free-block ((db hash-db) rsiz res)
  (with-slots (fbp_ fbpnum_ flock_) db
    (when (< fbpnum_ 1)
      (return-from fetch-free-block nil))
    (with-spinlock (flock_)
      (let ((fb (make-free-block :off #xffff :rsiz rsiz)))
        (loop for i in fbp_
              if (free-block< fb i)
                do (progn
                     (setf (free-block-off res) (free-block-off i)
                           (free-block-rsiz res) (free-block-rsiz i)
                           fbp_ (remove i fbp_))
                     ;; TODO escape_cursors(res->off, res->off + res->rsiz);
                     (return-from fetch-free-block t))))))
  nil)


(defmethod dump-empty-free-blocks ((db hash-db))
  (with-slots (fbpnum_ file_) db
    (when (< fbpnum_ 0)
      (return-from dump-empty-free-blocks t))
    (file-position file_ +hdbheadsiz+)
    (write-sequence (make-array 2 :element-type '(unsigned-byte 8) :initial-element 0)
                    file_)))



(defmethod db-open ((db hash-db) path)
  (with-slots (mlock_ file_ path_ head_ roff_ msiz_) db
    (with-spin-rw-lock (mlock_ t)
      (setf file_ (open path :direction :io :element-type '(unsigned-byte 8)
                        :if-exists :overwrite :if-does-not-exist :create)
            file_ (make-instance 'db-stream :base-stream file_ :mmap-size msiz_ :ext 1.5))
      ;; TODO ここでリカバリー処理が走る。
      (when (zerop (stream-length file_))
        (calc-meta db)
        (setf (head-chksum head_) (calc-checksum db))
        (setf (atomic-int-value (head-lsiz head_)) roff_)
        (stream-truncate file_ (atomic-int-value (head-lsiz head_)))
        (dump-meta db))
      (load-meta db)
      (calc-meta db)
      (unless (= (head-chksum head_) (calc-checksum db))
        (error "invalid module checksum."))
      (load-free-blocks db)
      (dump-empty-free-blocks db)
      (setf path_ path))))

(defmethod accept ((db hash-db) kbuf ksiz writable full empty)
  (with-slots (mlock_ head_ rlock_) db
    (with-spin-rw-lock (mlock_ nil)
      (let* ((hash (hash-record db kbuf ksiz))
             (pivot (fold-hash db hash))
             (bidx (mod hash (head-bnum head_)))
             (lidx (mod bidx +hdbrlockslot+)))
        (with-slotted-rw-lock (rlock_ lidx writable)
          (accept-impl db kbuf ksiz full empty bidx pivot nil)
          ;; TODO この後デフラグ処理をする。
          )))))



(defmethod accept-impl-find-record ((db hash-db)
                                    kbuf
                                    ksiz
                                    bidx
                                    pivot)
  (with-slots (comp_ linear_ width_) db
    (let ((off (get-backet db bidx))
          (entoff 0)
          (rbuf (make-array +hdbrecbufsiz+ :element-type '(unsigned-byte 8)))
          (record (make-record)))
      (loop while (> off 0)
            do (progn
                 (setf (record-off record) off)
                 (read-record db record rbuf)
                 (when (= (record-psiz record) #xffff)
                   (error "free block in the chain. ~a" record))
                 (let ((tpivot (if linear_
                                   pivot
                                   (fold-hash db
                                              (hash-record db
                                                           (record-kbuf record)
                                                           (record-ksiz record))))))
                   (cond ((> pivot tpivot)
                          ;;(break "(> pivot tpivot) ~a ~a ~a" pivot tpivot record)
                          (setf off (record-left record)
                                entoff (+ (record-off record) 2)))
                         ((< pivot tpivot)
                          ;;(break "(< pivot tpivot) ~a ~a ~a" pivot tpivot record)
                          (setf off (record-right record)
                                entoff (+ (record-off record) 2 width_)))
                         (t
                          (let ((kcmp (compare-keys db kbuf ksiz
                                                    (record-kbuf record) (record-ksiz record))))
                            (when (and linear_ (/= kcmp 0))
                              (setf kcmp 1))
                            (cond ((> kcmp 0)
                                   (setf off (record-left record)
                                         entoff (+ (record-off record) 2)))
                                  ((< kcmp 0)
                                   (setf off (record-right record)
                                         entoff (+ (record-off record) 2 width_)))
                                  (t
                                   (unless (record-vbuf record)
                                     (read-record-body db record))
                                   (return-from accept-impl-find-record
                                     (values t record rbuf entoff))))))))))
      (values nil record rbuf entoff))))

(defmethod accept-impl-full ((db hash-db)
                             kbuf
                             ksiz
                             full
                             bidx
                             isiter
                             rbuf
                             entoff
                             record)
  (with-slots (autosync_ autotran_ comp_ frgcnt_ linear_ head_ psiz_ tran_ width_) db
    (let ((vbuf (record-vbuf record))
          (vsiz (record-vsiz record)))
      (when comp_
        ;; TODO decompression
        )
      (setf (values vbuf vsiz)
            (funcall full kbuf ksiz vbuf vsiz))
      (case vbuf
        (:remove
           (write-free-block db
                             (record-off record)
                             (record-rsiz record)
                             rbuf)
           (insert-free-block db
                              (record-off record)
                              (record-rsiz record))
           (incf frgcnt_)
           (cut-chain db record rbuf bidx entoff)
           (atomic-int-add (head-count head_) -1))
        (:nop t)
        (t
           (let ((rsiz (calc-record-size db (record-ksiz record) vsiz)))
             (if (<= rsiz (record-rsiz record))
                 (progn
                   (setf (record-psiz record) (- (record-rsiz record)
                                                 rsiz)
                         (record-vsiz record) vsiz
                         (record-vbuf record) vbuf)
                   (adjust-record db record)
                   (write-record db record t))
                 (progn
                   (write-free-block db
                                     (record-off record)
                                     (record-rsiz record)
                                     rbuf)
                   (insert-free-block db
                                      (record-off record)
                                      (record-rsiz record))
                   (incf frgcnt_)
                   (let ((psiz (calc-record-padding db rsiz))
                         (over nil)
                         (fb (make-free-block)))
                     (setf (record-rsiz record) (+ rsiz psiz)
                           (record-psiz record) psiz
                           (record-vsiz record) vsiz
                           (record-vbuf record) vbuf)
                     (if (and (not isiter)
                              (fetch-free-block db
                                                (record-rsiz record)
                                                fb))
                         (progn
                           (setf (record-off record) (free-block-off fb)
                                 (record-rsiz record) (free-block-rsiz fb)
                                 (record-psiz record) (record-rsiz record)
                                 over t)
                           (adjust-record db record))
                         (setf (record-off record)
                               (atomic-int-add (head-lsiz head_)
                                               (record-rsiz record))))
                     (write-record db record over)
                     (unless over
                       (atomic-int-secure-least
                        psiz_
                        (+ (record-off record)
                           (record-rsiz record))))
                     (if (> entoff 0)
                         (set-chain db entoff (record-off record))
                         (set-bucket db bidx (record-off record))))))))))))

(defmethod accept-impl-empty ((db hash-db)
                              kbuf
                              ksiz
                              empty
                              bidx
                              entoff
                              record)
  (with-slots (autosync_ autotran_ comp_ frgcnt_ linear_ head_ psiz_ tran_ width_) db
    (multiple-value-bind (vbuf vsiz) (funcall empty kbuf ksiz)
      (case vbuf
        ((:nop :remove)
           t)
        (t
           (let ((atran nil))
             (when comp_
               ;; TODO compression
               )
             (when (and autotran_ (not tran_))
               (begin-auto-transaction db)
               (setf atran t))
             (let* ((rsiz (calc-record-size db ksiz vsiz))
                    (psiz (calc-record-padding db rsiz))
                    (over nil)
                    (fb (make-free-block)))
               (setf (record-rsiz record) (+ rsiz psiz)
                     (record-psiz record) psiz
                     (record-ksiz record) ksiz
                     (record-vsiz record) vsiz
                     (record-left record) 0
                     (record-right record) 0
                     (record-kbuf record) kbuf
                     (record-vbuf record) vbuf)
               (if (fetch-free-block db (record-rsiz record) fb)
                   (progn
                     (setf (record-off record) (free-block-off fb)
                           (record-rsiz record) (free-block-rsiz fb)
                           (record-psiz record) (- (record-rsiz record) rsiz)
                           over t)
                     (adjust-record db record))
                   (setf (record-off record) (atomic-int-add (head-lsiz head_) (record-rsiz record))))
               (write-record db record over)
               (unless over
                 (atomic-int-secure-least psiz_ (+ (record-off record) (record-rsiz record))))
               (if (> entoff 0)
                   (set-chain db entoff (record-off record))
                   (set-bucket db bidx (record-off record)))
               (atomic-int-add (head-count head_) 1)
               (cond (atran
                      (commit-auto-transaction db))
                     (autosync_
                      (synchronize-meta db))))))))))

(defmethod accept-impl ((db hash-db)
                        kbuf
                        ksiz
                        full
                        empty
                        bidx
                        pivot
                        isiter)
  (multiple-value-bind (found-p record rbuf entoff)
        (accept-impl-find-record db kbuf ksiz bidx pivot)
      (if found-p
          (accept-impl-full db kbuf ksiz full bidx isiter
                            rbuf entoff record)
          (accept-impl-empty db kbuf ksiz empty bidx entoff record))))

(defmethod synchronize-meta ((db hash-db))
  ;; TODO
  ;;(with-slots (flock_ file_) db
  ;;  (with-spinlock (flock_)
  ;;    (let ((err nil))
  ;;      (unless (dump-meta db)
  ;;        (setf err t))
  ;;      (unless (synchronize file_ t)
  ;;        (setf err t))
  ;;      (not err)))))
  )

(defmethod begin-auto-transaction ((db hash-db))
  )

(defmethod commit-transaction ((db hash-db))
  )

(defmethod commit-auto-transaction ((db hash-db))
  )

(defmethod db-close ((db hash-db))
  (with-slots (file_ mlock_) db
    (with-spin-rw-lock (mlock_ t)
      (dump-empty-free-blocks db)
      (dump-meta db)
      (close file_))))

(defun test-hash-db ()
  (time
   (let ((db (make-instance 'hash-db)))
     (db-open db "/tmp/foo")
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
            (setf (value db "aaa") "a"))
       (db-close db)))))