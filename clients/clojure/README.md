# kafka-clj
Clojure client for [Kafka](http://sna-projects.com/kafka/) a distributed publish/subscribe messaging system.

## Quick Start

Download and start [Kafka](http://sna-projects.com/kafka/quickstart.php). 

Pull dependencies with [Leiningen](https://github.com/technomancy/leiningen)

    $ lein deps

Run examples

    $ lein run-example \{1..3\}

## Usage

### Producing messages

    (with-open [p (producer "localhost" 9092)]
      (produce p "test" 0 "Message 1")
      (produce p "test" 0 ["Message 2" "Message 3"]))

_producer_ takes an optional configuration map that supports following configuration attributes:

Producer options

- :multi-produce       - if true, the messages are enqueued up to :max-queue-size or :max-produce-time timeout is reached, which applies first. Otherwise the messages are send immediately. Default is false.
- :max-queue-size      - max accumulated messages size till messages are send, default is 200.
- :max-produce-time    - max timeout between send attempts, default 30 sec.

Channel options:

- :receive-buffer-size - receive socket buffer size, default 65536.
- :send-buffer-size    - send socket buffer size, default 65536.
- :socket-timeout      - socket timeout.

### Consuming messages

    (with-open [c (consumer "localhost" 9092)]
      (let [offs (offsets c "test" 0 -1 10)]
        (consume c "test" 0 (last offs) 1000000)))

You can provide a configuration map to the _consumer_ function with following options:

- :multi-fetch         - if true, fetches messages for all consumer sequences, default true.
- :receive-buffer-size - receive socket buffer size, default 65536.
- :send-buffer-size    - send socket buffer size, default 65536.
- :socket-timeout      - socket timeout.
 
### Consumer sequence

The API provides a sequence interface that allows to consume messages with any sequence operator. One consumer can manage multiple sequences, each for one topic/partition respectively. Messages are fetch lazy and for all sequences at once. Every sequence can be configures as blocking or non-blocking.

    (with-open [c (consumer "localhost" 9092)]
      (doseq [m (consume-seq c "test")]
        (println m)))

Beware of side effects when using _consumer-seq_. As of now sequences are transient, for example the expression

    (let [c1 (consume-seq c "test")
          c2 (rest (consume-seq c "test"))]
      (= (first c1) (first c2))
    
will evaluate to true.

_consume-seq_ takes an optional map with following options:

- :blocking       - default false, sequence returns nil the first time fetch does not return new messages. If set to true, the sequence tries to fetch new messages :repeat-count times every :repeat-timeout milliseconds. 
- :repeat-count   - number of attempts to fetch new messages before terminating, default 10.
- :repeat-timeout _ wait time in milliseconds between fetch attempts, default 1000.
- :offset         - initialized to highest offset if not provided.
- :max-size       - max result message size, default 1000000.

### Serialization

Load namespace _kafka.print_ for basic print_dup -> read-string serialization or _kafka.serializeable_ for Java object serialization.
Implement Pack and Unpack protocols for custom serialization.

Problems, questions? Email adam.smyczek \_at\_ gmail.com.

