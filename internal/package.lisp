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
           #:atomic-int
           #:atomic-int-value
           #:atomic-int-add
           #:atomic-int-secure-least

           #:db-stream
           #:stream-truncate
           #:stream-length

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
           #:string-to-octets
           #:octets-to-string
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
