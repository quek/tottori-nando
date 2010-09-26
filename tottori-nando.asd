;;;; -*- Mode: LISP; -*-
(asdf:defsystem :tottori-nando
  :version "0.0.0"
  :serial t
  :components ((:module "internal"
                        :serial t
                        :components ((:file "package")
                                     (:file "util")
                                     (:file "api")
                                     (:file "internal")
                                     (:file "thread")
                                     (:file "file")
                                     (:file "basic-db")))
               (:module "api"
                        :serial t
                        :components ((:file "package")))
               (:module "hash"
                        :serial t
                        :components ((:file "package")
                                     (:file "hash-db")))
               (:module "skip-list"
                        :serial t
                        :components ((:file "skip-list-memory-db-package")
                                     (:file "skip-list-memory-db")
                                     (:file "skip-list-db-package")
                                     (:file "skip-list-db"))))
  :depends-on (:alexandria
               :anaphora
               :series))
