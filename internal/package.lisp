(defpackage :tottori-nando.internal
    (:use :cl :anaphora :series)
  (:shadowing-import-from :series let let* multiple-value-bind funcall defun)
  (:export #:uint64
           #:ubyte
           #:octets

           #:spin-rw-lock
           #:with-spin-rw-lock
           #:make-slotted-spin-rw-lock
           #:with-slotted-rw-lock
           #:make-spinlock
           #:with-spinlock
           #:make-recursive-spinlock
           #:with-recursive-spinlock
           #:atomic-int
           #:atomic-int-value
           #:atomic-int-add
           #:atomic-int-secure-least

           #:open-mmap-stream
           #:mmap-stream
           #:mmap-stream-sap
           #:stream-truncate
           #:stream-length
           #:read-seq-at
           #:write-seq-at
           #:write-8-at
           #:write-16-at
           #:write-32-at
           #:write-64-at
           #:read-8-at
           #:read-16-at
           #:read-32-at
           #:read-64-at

           #:heap
           #:make-heap
           #:heap-start
           #:heap-end
           #:heap-fragments-offset
           #:heap-fragments
           #:heap-stream
           #:alloc
           #:free
           #:dump-fragments
           #:load-fragments

           #:n++
           #:scan-byte
           #:collect-byte
           #:vector-push-byte-extend
           #:sym
           #:def-byte-struct
           #:write-var-num
           #:read-var-num
           #:write-fixnum
           #:read-fixnum
           #:hash-murmur
           #:copy-sap-to-sap
           #:copy-sap-to-vector
           #:copy-vector-to-sap
           #:string-to-octets
           #:octets-to-string
           #:make-buffer
           #:bytes-into
           #:from-bytes
           #:ref-8
           #:ref-16
           #:ref-32
           #:ref-64
           #:gref

           #:+formatver+
           #:+librev+
           #:+libver+
           #:basic-db

           #:accept

           #:db
           #:hash-db
           #:db-open
           #:db-close
           #:value
           #:get-op
           #:get-op*
           #:set-op
           #:set-op*
           #:add-op
           #:add-op*
           #:replace-op
           #:replace-op*
           #:append-op
           #:append-op*
           #:inc-op
           #:inc-op*
           #:cas-op
           #:cas-op*
           #:delete-op
           #:delete-op*))

(series::install :pkg :tottori-nando.internal)
