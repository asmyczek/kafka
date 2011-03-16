(defproject kafka-clj "0.2-SNAPSHOT"
  :description "Kafka client for Clojure."
  :url          "http://sna-projects.com/kafka/"
  :dependencies [[org.clojure/clojure	"1.2.0"]
                 [org.clojure/clojure-contrib	"1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :disable-deps-clean false
  :warn-on-reflection true
  :source-path "src"
  :test-path "test"
  :mailing-list {:name "Kafka mailing list"
                 :archive "http://groups.google.com/group/kafka-dev"}
  :license {:name "Apache License - Version 2.0"
            :url  "http://www.apache.org/licenses/"})

