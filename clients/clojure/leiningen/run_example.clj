(ns leiningen.run-example
  (:use [leiningen.compile :only (eval-in-project)]))

(def examples-map
  {"1" {:name "Simple producer/consumer example."   :namespace 'examples.simple}
   "2" {:name "Multi-produce/multi-fetch example."   :namespace 'examples.multi}})
   
(defn run-example
  [project & args]
  (if args
    (if-let [exp (examples-map (first args))]
      (eval-in-project project
        `(let [ns# (:namespace '~exp)]
           (println "------------------------------------------------")
           (println "Running:" (:name '~exp))
           (println "------------------------------------------------")
           (require ns#)
           ((get (ns-publics ns#) (symbol "run")))))
      (println (str "Invalid example no. Expecting {1.." (count examples-map) "}.")))
    (println (str "Provide example no. {1.." (count examples-map) "}."))))

