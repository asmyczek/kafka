(ns #^{:doc "Kafka producer,
            provides the producer factory."}
  kafka.producer
  (:use (kafka types utils buffer)
        (clojure.contrib [logging :only (debug info error fatal)]))
  (:import (kafka.types Message)))

(defn- config
  "Producer configuration map."
  [topic partition messages]
  {:topic     topic
   :partition partition
   :messages  (if (sequential? messages)
                messages
                [messages])})

(defn- produce-messages
  "Wirtes one producer into the buffer."
  [config]
  (length-encoded short                 ; topic size
    (put (:topic config)))              ; topic
  (put (int (:partition config)))       ; partition
  (length-encoded int                   ; messages size
    (doseq [m (:messages config)]
      (let [^Message pm (pack m)]
        (length-encoded int             ; message size
          (put (byte 0))                ; magic
          (with-put 4 crc32-int         ; crc
            (put (.message pm))))))))   ; message

(defn- send-messages
  "Send one producer messages."
  [channel config opts]
  (log debug config "Sending single producer request.")
  (let [size (or (:send-buffer-size opts) *default-buffer-size*)]
    (with-buffer (buffer size)
      (length-encoded int                     ; request size
        (put (short 0))                       ; produce request type
        (produce-messages config))
      (flip)
      (write-to channel))))

(defn- take-queue
  "Takes all enqueued massages for sending. Clears the queue."
  [queue]
  (dosync
    (let [q @queue]
      (alter queue (fn [_] (seq [])))
      q)))

(defn- send-queue
  "Send all enqueued producers."
  [channel queue opts]
  (if (not (empty? @queue))
    (let [q (reverse (take-queue queue))]
      (debug (str "Sending " (count q) " enqueued producer requests."))
      (let [size (or (:send-buffer-size opts) *default-buffer-size*)]
        (with-buffer (buffer size)
          (length-encoded int       ; request size
            (put (short 3))         ; Multi-produce request type
            (put (short (count q))) ; producer count
            (doseq [cfg q]
              (produce-messages cfg)))
          (flip)
          (write-to channel))))))

(defn- start-producer-thread
  "Starts producer thread. The thread sends all enqueued massages
  every :max-produce-time milliseconds. It returns a handler function
  that stops the thread when called."
  [channel queue opts]
  (let [sleep (or (:max-produce-time opts) 30000)
        active (atom true)]
    (debug "Starting producer thread.")
    (.start
      (Thread.
        (fn []
           (if @active
             (do
               (send-queue channel queue opts)
               (Thread/sleep sleep)
               (recur))
             (info "Stopped producer thread.")))))
    (fn []
      (reset! active false))))

(defn- enqueue
  "Enqueue a producer."
  [channel queue config opts]
  (let [max-size (or (:max-queue-size opts) 200)
        q        (dosync (alter queue conj config))
        size     (apply + (map #(count (:messages %)) q))]
    (log debug config "Enqueue message " size)
    (if (>= size max-size)
      (send-queue channel queue opts))))

(defn producer
  "Creates a new producer for host:port and optional configuration map.
  Supported configuration options are:

  Producer options:
  :multi-produce       - if true, the messages are enqueued up to :max-queue-size
                         or :max-produce-time timeout, which applies first. 
                         Otherwise the messages are send immediately.
  :max-queue-size      - max accumulates messages size till messages are send, default is 200.
  :max-produce-time    - max timeout between send attempts, default 30 sec.

  Channel options:
  :receive-buffer-size - receive socket buffer size, default 65536.
  :send-buffer-size    - send socket buffer size, default 65536.
  :socket-timeout      - socket timeout."
  [host port & [opts]]
  (let [channel       (new-channel host port opts)
        multi-produce (or (:multi-produce opts) false)
        queue         (ref (seq []))
        stop-handler  (if multi-produce
                        (start-producer-thread channel queue opts)
                        (fn []))]
    (reify Producer
      (produce [this topic partition messages]
               (let [cfg (config topic partition messages)]
                 (if multi-produce
                   (enqueue channel queue cfg opts)
                   (send-messages channel cfg opts))))
      (close [this]
             (stop-handler)
             (close-channel channel)))))

