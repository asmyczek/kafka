(ns #^{:doc "Producer/Consumer example."}
  kafka.example
  (:use (clojure.contrib [logging :only (error info)])
        (kafka types kafka print)))

(defmacro thread
  "Executes body in a thread, logs exceptions."
  [ & body]
  `(future
     (try
       ~@body
       (catch Exception e#
         (error "Exception." e#)))))

(defn start-consumer
  []
  (thread
    (with-open [c (consumer "localhost" 9092)]
      (doseq [m (consume-seq c "test" 0 {:blocking true})]
        (info (str "Consumed <-- " m))))
    (info "Finished consuming.")))

(defn start-producer
  []
  (thread
    (with-open [p (producer "localhost" 9092)]
      (doseq [i (range 1 20)]
        (let [m (str "Message " i)]
          (produce p "test" 0 m)
          (info (str "Produced --> " m))
          (Thread/sleep 1000))))
    (info "Finished producing.")))

(defn run
  []
  (start-consumer)
  (start-producer))

