(ns #^{:doc "Utils used by examples."}
  examples.utils
  (:use (clojure.contrib [logging :only (error)])))

(defmacro thread
  "Executes body in a thread, logs exceptions."
  [ & body]
  `(future
     (try
       ~@body
       (catch Exception e#
         (error "Exception." e#)))))

(defmacro mapall
  "Shortcut for eager map evaluation."
  [ & body ]
  `(doall (map ~@body)))

