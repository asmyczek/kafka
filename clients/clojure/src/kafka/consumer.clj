(ns #^{:doc "Consumer factory."}
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
   :offset    (atom offset)
   :queue     (atom (seq []))})

(defmacro consumer-key
  [config]
  `(str (:topic ~config) "-" (:partition ~config)))

(defmacro log
  [level config & msg]
  `(~level (str (consumer-key ~config) ": " ~@msg)))

(defn- response-size
  "Read first four bytes from channel as an integer."
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
  [config]
  (length-encoded short           ; topic size
    (put (:topic config)))        ; topic
  (put (int (:partition config))) ; partition
  (put (long @(:offset config)))  ; offset
  (put (int (:max-size config)))) ; max size

(defn- fetch-request
  "Fetch messages request."
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
  "Multifetch messages request."
  [channel config-map opts]
  (let [size (or (:send-buffer-size opts) *default-buffer-size*)]
    (with-buffer (buffer size)
      (length-encoded int                  ; request size
        (put (short 2))                    ; multifetch request type
        (put (short (count config-map)))   ; fetch consumer count
        (doseq [config (vals (sort config-map))]
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
  "Read response from buffer."
  [config]
  (loop [off @(:offset config) msg []]
    (if (has-remaining)
      (let [m-size  (get-int)  ; message size
            magic   (get-byte) ; magic
            crc     (get-int)  ; crc
            message (get-array (- m-size 5))]
        (recur (+ off m-size 4) (conj msg (unpack (Message. message)))))
      (do
        (log debug config "Fetched " (count msg) " messages.")
        (log debug config "New offset " off ".")
        (swap! (:queue config) #(reduce conj % (reverse msg)))
        (reset! (:offset config) off)))))

(defn- fetch-messages
  "Message fetch, writes messages into the config queue."
  [channel config]
  (fetch-request channel config)
  (read-response channel
    (read-message-set config)))

(defn- multifetch-messages
  "Multifetch messages."
  [channel config-map opts]
  (multifetch-request channel config-map opts)
  (read-response channel
    (doseq [config (vals (sort config-map))]
      (let [size (get-int)]  ; one message set size
        (when (> size 0)
          (with-error-code
            (slice (- size 2) (read-message-set config))))))))

;
; Consumer sequence
;

(defn- seq-fetch
  "Non-blocking fetch function used by consumer sequence."
  [channel config-map opts]
  (fn [config]
    (if (> (count @config-map) 1)
      (multifetch-messages channel @config-map opts)
      (fetch-messages channel config))))

(defn- blocking-seq-fetch
  "Blocking fetch function used by consumer sequence."
  [channel config-map opts]
  (let [repeat-count   (or (:repeat-count opts) 10)
        repeat-timeout (or (:repeat-timeout opts) 1000)]
    (fn [config]
      (loop [c repeat-count]
        (if (> c 0)
          (do 
            ((seq-fetch channel config-map opts) config)
            (when (empty? @(:queue config))
              (Thread/sleep repeat-timeout)
              (recur (dec c))))
          (log debug config "Stopping blocking seq fetch."))))))

(defmacro fetch-queue
  [config fetch-fn]
  `(if (empty? @(:queue ~config))
    (~fetch-fn ~config)))

(defn- consumer-seq
  "Sequence constructor."
  [config fetch-fn]
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
        (fetch-queue config fetch-fn)
        (first @(:queue config)))
      (next [this]
        (swap! (:queue config) rest)
        (fetch-queue config fetch-fn)
        (if (not (empty? @(:queue config))) this))
      (more [this]
        (swap! (:queue config) rest)
        (fetch-queue config fetch-fn)
        (if (empty? @(:queue config)) (empty) this))
    Object
    (toString [this]
      (str "ConsumerQueue"))))

;
; Consumer factory
;

(defn consumer
  "Consumer factory. See new-channel for list of supported options."
  [host port & [opts]]
  (let [channel    (new-channel host port opts)
        multifetch (or (:multifetch opts) true)
        fetch-map  (atom {})]
    (reify Consumer
      (consume [this topic partition offset max-size]
        (let [cfg (config topic partition offset max-size)]
          (fetch-messages channel cfg)
          [@(:offset cfg) @(:queue cfg)]))
      
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
              cfg-key  (consumer-key cfg)]
          (when (@fetch-map cfg-key)
            (throw (Exception. (str "Already consuming " cfg-key "."))))
          (let [cfg-map  (if multifetch
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

