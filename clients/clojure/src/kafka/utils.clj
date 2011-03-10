(ns #^{:doc "General utilities."}
  kafka.utils
  (:use (clojure.contrib [logging :only (debug info error fatal)]))
  (:import (java.nio.channels SocketChannel)
           (java.net InetSocketAddress)
           (java.util.zip CRC32)))

(def #^{:doc "Default buffer size used by all buffers."}
  *default-buffer-size* 65536)

(defn crc32-int
  "CRC on a byte array."
  [^bytes ba]
  (let [crc (doto (CRC32.) (.update ba))
        lv  (.getValue crc)]
    (.intValue (bit-and lv 0xffffffff))))

(defn new-channel
  "Create and setup a new channel for a host name, port and options.
  Supported options:
  :receive-buffer-size - receive socket buffer size, default 65536.
  :send-buffer-size    - send socket buffer size, default 65536.
  :socket-timeout      - socket timeout."
  [^String host ^Integer port opts]
  (let [receive-buf-size (or (:receive-buffer-size opts) *default-buffer-size*)
        send-buf-size    (or (:send-buffer-size opts) *default-buffer-size*)
        so-timeout       (or (:socket-timeout opts) 60000)
        ch (SocketChannel/open)]
    (doto (.socket ch)
      (.setReceiveBufferSize receive-buf-size)
      (.setSendBufferSize send-buf-size)
      (.setSoTimeout so-timeout))
    (doto ch
      (.configureBlocking true)
      (.connect (InetSocketAddress. host port)))))

(defn close-channel
  "Close the channel and socket."
  [^SocketChannel channel]
  (.close channel)
  (.close (.socket channel)))

(defmacro client-key
  "Generates key string from topic/partition."
  [config]
  `(str (:topic ~config) "-" (:partition ~config)))

(defmacro log
  "Log helper."
  [level config & msg]
  `(~level (str (client-key ~config) ": " ~@msg)))

