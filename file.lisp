(in-package :tottori-nando)

(defclass db-stream (sb-gray:fundamental-binary-input-stream
                     sb-gray:fundamental-binary-output-stream)
  ())

(defmethod synchronize (stream hard)
  t)