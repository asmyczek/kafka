(ns #^{:doc "Multi-produce/fetch example."}
  examples.multi
  (:use (clojure.contrib [logging :only (info)])
        (kafka types producer consumer print)
        (examples utils)))

(def *repeat-count* 5)

(defn start-consumer
  [ & topics ]
  (thread
    (with-open [c (consumer "localhost" 9092)]
      (loop [loop-count *repeat-count*
             consumers  (mapall #(consume-seq c % 0 {}) topics)]
        (when (> loop-count 0)
          (Thread/sleep 500)
          (let [results  (mapall #(vector (first %) %) consumers)
                messages (filter (comp not nil? first) results)]
            (doseq [m messages]
              (info (str "Consumed <-- " (first m) " - " (second m))))
            (recur (if (empty? messages)
                     (dec loop-count)
                     *repeat-count*)
                   (mapall (comp rest second) results)))))
      (info "Finished consuming."))))

(defn start-producer
  [topic]
  (thread
    (with-open [p (producer "localhost" 9092)]
      (doseq [i (range 1 20)]
        (let [m (str "Message " i)]
          (produce p topic 0 m)
          (info (str topic " produced --> " m))
          (Thread/sleep 500))))
    (info (str "Finished producer " topic "."))))

(defn run
  []
  (let [c  (start-consumer "topic 1" "topic 2")
        p1 (start-producer "topic 1")
        p2 (start-producer "topic 2")]
    (and @c @p1 @p2)
    (info "Done!")))

