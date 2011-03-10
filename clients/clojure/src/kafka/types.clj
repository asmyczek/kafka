(ns #^{:doc "kafka-clj types."}
  kafka.types)

(deftype #^{:doc "Message type, a wrapper around a byte array."}
  Message [^bytes message])

(defprotocol Pack
  "Pack protocol converts an object into a Message."
  (pack [this] "Convert object to a Message."))

(defprotocol Unpack
  "Unpack protocol, reads an object from a Message."
  (unpack [^Message this] "Read an object from the message."))

(defprotocol Producer
  "Producer protocol."
  (produce [this topic partition messages] "Send message[s]. Messages objects have to implement Pack protocol.")
  (close   [this]                          "Closes the producer, socket and channel."))

(defprotocol Consumer
  "Consumer protocol."
  (consume      [this topic partition offset max-size]  "Fetch messages. Returns a pair [last-offset, message sequence]")
  (offsets      [this topic partition time max-offsets] "Query offsets. Returns offsets seq.")

  (consume-seq  [this topic partition]                  
                [this topic partition opts]
    "Creates a sequence over the consumer. Supported options are:
    :blocking       - default false, sequence returns nil the first time fetch does not return new messages.
                      If set to true, the sequence tries to fetch new messages :repeat-count times every 
                      :repeat-timeout milliseconds. 
    :repeat-count   - number of attempts to fetch new messages before terminating, default 10.
    :repeat-timeout - wait time in milliseconds between fetch attempts, default 1000.
    :offset         - initialized to highest offset if not provided.
    :max-size       - max result message size, default 1000000.")

  (close        [this] "Close the consumer, socket and channel."))

