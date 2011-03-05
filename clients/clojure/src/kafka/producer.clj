(ns #^{:doc "Producer factory."}
  kafka.producer
  (:use (kafka types utils buffer)
        (clojure.contrib [logging :only (debug info error fatal)]))
  (:import (kafka.types Message)))

(defn- send-message
  "Send messages."
  [channel topic partition messages opts]
  (let [size (or (:send-buffer-size opts) *default-buffer-size*)]
    (with-buffer (buffer size)
        (length-encoded int                   ; request size
          (put (short 0))                     ; produce request type
          (length-encoded short               ; topic size
            (put topic))                      ; topic
          (put (int partition))               ; partition
          (length-encoded int                 ; messages size
            (doseq [m messages]
              (let [^Message pm (pack m)]
                (length-encoded int           ; message size
                  (put (byte 0))              ; magic
                  (with-put 4 crc32-int       ; crc
                    (put (.message pm)))))))) ; message
        (flip)
        (write-to channel))))

(defn producer
  "Producer factory. See new-channel for list of supported options."
  [host port & [opts]]
  (let [channel (new-channel host port opts)]
    (reify Producer
      (produce [this topic partition messages]
               (let [msg (if (sequential? messages) messages [messages])]
                 (send-message channel topic partition msg opts)))
      (close [this]
             (close-channel channel)))))

