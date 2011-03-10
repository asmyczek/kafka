(ns #^{:doc "Simple producer consumer example."}
  examples.simple
  (:use (clojure.contrib [logging :only (info)])
        (kafka types producer consumer print)
        (examples utils)))

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


