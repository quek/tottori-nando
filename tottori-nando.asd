;;;; -*- Mode: LISP; -*-
(asdf:defsystem :tottori-nando
  :version "0.0.0"
  :serial t
  :components ((:module "internal"
                        :serial t
                        :components ((:file "package")
                                     (:file "type")
                                     (:file "util")
                                     (:file "api")
                                     (:file "thread")
                                     (:file "file")
                                     (:file "heap")
                                     (:file "basic-db")))
               (:module "api"
                        :serial t
                        :components ((:file "package")))
               (:module "hash"
                        :serial t
                        :components ((:file "package")
                                     (:file "hash-db")))
               (:module "skip-list-memory"
                        :serial t
                        :components ((:file "package")
                                     (:file "skip-list-memory-db")))
               (:module "skip-list"
                        :serial t
                        :components ((:file "package")
                                     (:file "skip-list-db")))
               (:module "object-store"
                        :serial t
                        :components ((:file "package")
                                     (:file "object-store"))))
  :depends-on (:alexandria
               :anaphora
               :series))
