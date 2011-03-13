(ns #^{:doc "Kafka consumer,
            provides consumer factory."}
  kafka.consumer
  (:use (kafka types utils buffer)
        (clojure.contrib [logging :only (debug info error fatal)]))
  (:import (kafka.types Message)))

; 
; Utils
;

(defn- config
  "Consumer configuration map."
  [topic partition offset max-size]
  {:topic     topic
   :partition partition
   :max-size  max-size
   :offset    offset
   :queue     nil})

(defn- response-size
  "Reads response size, the first four bytes
  from the channel and returns this as integer."
  [channel]
  (with-buffer (buffer 4)
    (read-completely-from channel)
    (flip)
    (get-int)))

(defmacro with-error-code
  "Convenience response error code check."
  [ & body]
  `(let [error-code# (get-short)] ; error code
     (if (not= error-code# 0)
       (error (str "Request returned error code: " error-code# "."))
       ~@body)))

(defmacro read-response
  "General read response wrapper macro."
  [ channel & body ]
  `(let [size# (response-size ~channel)]
     (with-buffer (buffer size#)
        (read-completely-from ~channel)
        (flip)
        (with-error-code
          ~@body))))

;
; Requests
;

(defn- fetch-offset-request
  "Fetch offsets request."
  [channel topic partition time max-offsets]
  (let [size     (+ 24 (count topic))]
    (with-buffer (buffer size)
      (length-encoded int         ; request size
        (put (short 4))           ; offsets request type
        (length-encoded short     ; topic size
          (put topic))            ; topic
        (put (int partition))     ; partition
        (put (long time))         ; time
        (put (int max-offsets)))  ; max-offsets
        (flip)
        (write-to channel))))

(defn- fetch-message-set
  "Fetch single message set used by fetch and multi-fetch."
  [config]
  (length-encoded short           ; topic size
    (put (:topic config)))        ; topic
  (put (int (:partition config))) ; partition
  (put (long (:offset config)))  ; offset
  (put (int (:max-size config)))) ; max size

(defn- fetch-request
  "Single fetch messages request."
  [channel config]
  (let [topic (:topic config)
        size  (+ 24 (count topic))]
    (with-buffer (buffer size)
      (length-encoded int               ; request size
        (put (short 1))                 ; fetch request type
        (fetch-message-set config))
        (flip)
        (write-to channel))))

(defn- multifetch-request
  "Multi-fetch messages request."
  [channel config-map opts]
  (let [size (or (:send-buffer-size opts) *default-buffer-size*)]
    (with-buffer (buffer size)
      (length-encoded int                  ; request size
        (put (short 2))                    ; multi-fetch request type
        (put (short (count config-map)))   ; fetch consumer count
        (doseq [config (vals (sort-by first config-map))]
          (fetch-message-set config)))
        (flip)
        (write-to channel))))

;
; Fetch functions
;

(defn- fetch-offsets
  "Fetch offsets as an integer sequence."
  [channel topic partition time max-offsets]
  (fetch-offset-request channel topic partition time max-offsets)
  (read-response channel
    (loop [c (get-int) res []]
      (if (> c 0)
        (recur (dec c) (conj res (get-long)))
        (doall res)))))

(defn- read-message-set
  "Read message set response from buffer."
  [config]
  (loop [off (:offset config) msg []]
    (if (has-remaining)
      (let [m-size  (get-int)  ; message size
            magic   (get-byte) ; magic
            crc     (get-int)  ; crc
            message (get-array (- m-size 5))]
        (recur (+ off m-size 4) (conj msg (unpack (Message. message)))))
      (do
        (log debug config "Fetched " (count msg) " messages.")
        (log debug config "New offset " off ".")
        (assoc config :queue (reduce conj (:queue config) (reverse msg))
                      :offset off)))))

(defn- fetch-messages
  "Fetches and writes messages into the config queue."
  [channel config]
  (fetch-request channel config)
  (read-response channel
    (read-message-set config)))

(defn- multi-fetch-messages
  "Multifetch messages."
  [channel config-map opts]
  (multifetch-request channel config-map opts)
  (read-response channel
    (loop [config (vals (sort-by first config-map)) res {}]
      (if (empty? config)
        res
        (let [size (get-int)]  ; one message set size
          (if (> size 0)
            (with-error-code
              (let [cfg (slice (- size 2) (read-message-set (first config)))]
                (recur (next config) (assoc res (client-key cfg) cfg))))
            (let [cfg (first config)]
              (recur (next config) (assoc res (client-key cfg) cfg)))))))))

;
; Consumer sequence
;

(defn- seq-fetch
  "Non-blocking fetch function used by consumer sequence."
  [channel config-map opts]
  (fn [config]
    (if (> (count @config-map) 1)
      (let [cfg-map (multi-fetch-messages channel @config-map opts)]
        (reset! config-map cfg-map)
        (cfg-map (client-key config)))
      (fetch-messages channel config))))

(defn- blocking-seq-fetch
  "Blocking fetch function used by consumer sequence."
  [channel config-map opts]
  (let [repeat-count   (or (:repeat-count opts) 10)
        repeat-timeout (or (:repeat-timeout opts) 1000)]
    (fn [config]
      (loop [c repeat-count]
        (if (> c 0)
          (let [cfg ((seq-fetch channel config-map opts) config)]
            (if (empty? (:queue cfg))
              (do
                (Thread/sleep repeat-timeout)
                (recur (dec c)))
              cfg))
          (log debug config "Stopping blocking seq fetch."))))))

(defn- fetch-queue
  "Fetch next messages."
  [cfg fetch-fn]
  (if (empty? (:queue @cfg))
    (if-let [new-cfg (fetch-fn @cfg)]
      (reset! cfg new-cfg))))

(defn- consumer-seq
  "Sequence constructor."
  [config fetch-fn]
  (let [cfg (atom config)]
    (reify
      clojure.lang.IPersistentCollection
        (seq [this]    this)
        (cons [this _] (throw (Exception. "cons not supported for consumer sequence.")))
        (empty [this]  nil)
        (equiv [this o]
          (fatal "Implement equiv for consumer seq!")
          false)
      clojure.lang.ISeq
        (first [this]
          (fetch-queue cfg fetch-fn)
          (first (:queue @cfg)))
        (next [this]
          (fetch-queue cfg fetch-fn)
          (if (not (empty? (:queue @cfg)))
            (consumer-seq (update-in @cfg [:queue] rest) fetch-fn)))
        (more [this]
          (consumer-seq (update-in @cfg [:queue] rest) fetch-fn))
      Object
      (toString [this]
        (str (client-key @cfg))))))

;
; Consumer factory
;

(defn consumer
  "Consumer factory listening on host:port and optional configuration parameters.
  Following options are supported:
  :multi-fetch         - if true, fetches messages for all consumer sequences.
  :receive-buffer-size - receive socket buffer size, default 65536.
  :send-buffer-size    - send socket buffer size, default 65536.
  :socket-timeout      - socket timeout."
  [host port & [opts]]
  (let [channel    (new-channel host port opts)
        multi-fetch (or (:multi-fetch opts) true)
        fetch-map  (atom {})]
    (reify Consumer
      (consume [this topic partition offset max-size]
        (let [cfg (config topic partition offset max-size)
              cfg (fetch-messages channel cfg)]
          [(:offset cfg) (:queue cfg)]))
      
      (offsets [this topic partition time max-offsets]
        (fetch-offsets channel topic partition time max-offsets))

      (consume-seq [this topic partition]
        (let [[offset] (or (fetch-offsets channel topic partition -1 1) 0)]
          (consume-seq this topic partition {:offset offset})))

      (consume-seq [this topic partition opts]
        (let [[offset] (or (:offset opts)
                           (fetch-offsets channel topic partition -1 1)
                           0)
              max-size (or (:max-size opts) 1000000)
              cfg      (config topic partition offset max-size)
              cfg-key  (client-key cfg)]
          (when (@fetch-map cfg-key)
            (throw (Exception. (str "Already consuming " cfg-key "."))))
          (let [cfg-map  (if multi-fetch
                           (do
                             (swap! fetch-map assoc cfg-key cfg)
                             fetch-map)
                           (atom {cfg-key cfg}))
                fetch-fn (if (:blocking opts)
                           (blocking-seq-fetch channel cfg-map opts)
                           (seq-fetch channel cfg-map opts))]
            (log debug cfg "Initializing last offset to " offset ".")
            (consumer-seq cfg fetch-fn))))

      (close [this]
        (close-channel channel)))))

