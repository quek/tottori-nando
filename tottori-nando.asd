;;;; -*- Mode: LISP; -*-
(asdf:defsystem :tottori-nando
  :version "0.0.0"
  :serial t
  :components ((:file "package")
               (:file "util")
               (:file "thread")
               (:file "file")
               (:file "basic-db")
               (:file "hash-db"))
  :depends-on (:alexandria
               :anaphora
               :series))
