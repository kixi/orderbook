(ns orderbook.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [org.httpkit.client :as http]
            [clojure.core.async :as async]
            [clojure.string :as s]
            [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.exchange :as le]
            [langohr.basic :as lb]
            [com.stuartsierra.component :as component]
            [orderbook.async :as asvc]))

(def service-name "YahooFeederService")

(defn freq->ms [frequency]
  (* 1000 (/ 1 frequency)))

(defn heartbeat [{:keys [chan service-name frequency]
                  :or {frequency  0.1}}]
  (async/go-loop []
    (let [msg  {:type :heartbeat
                :service service-name
                :timestamp (java.util.Date.)}]
      (log/info "Sending hearbeat: " msg)
      (async/>! chan msg))
    (async/<! (async/timeout (freq->ms frequency)))
    (recur)))

(defrecord HeartbeatComponent [service-name chan frequency]
  component/Lifecycle
  (start [this]
    (log/info "Starting heartbeat component")
    (heartbeat  {:chan chan :service-name service-name :frequency frequency})
    this)

  (stop [this]
    this
    (log/info "Stopping hearbeat component")))

(defrecord RabbitMQConnectionComponent []
  component/Lifecycle
  (start [this]
    (log/info "Starting RabbitMQ Connection component")
    (let [connection (rmq/connect {:automatically-recover true :automatically-recover-topology :true})
          channel (lch/open connection)]
      (assoc this :connection connection :channel channel)))
  (stop [this]
    this))

(defn command-listener-loc []
  (let [cmd-ch (async/chan)]
    (async/go-loop []
      (let [{:keys [payload ch meta] :as msg} (async/<! cmd-ch)
            {:keys [delivery-tag content-type type]} meta]
        (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                         (String. payload "UTF-8") delivery-tag content-type type)))
      (recur))
    cmd-ch))

(defn command-listener [{:keys [channel queue]}]
  (let [cmd-ch (command-listener-loc)]
    (letfn [(message-handler [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
              (async/put! cmd-ch {:ch ch :meta meta :payload payload}))]
      (lc/subscribe channel queue message-handler {:auto-ack true}))))

(defrecord CommandListenerComponent [mq-connection queue]
  component/Lifecycle
  (start [this]
    (command-listener {:channel (:channel mq-connection) :queue queue})
    this)
  (stop [this]
    this))

(defn get-url-4-symbol [ticker-symbol]
  (str "http://chartapi.finance.yahoo.com/instrument/1.0/"
       ticker-symbol
       "/chartdata;type=quote;range=1d/csv"))

(defn fetch-data [ticker-symbol ch]
  (http/get (get-url-4-symbol ticker-symbol)
            (fn [response]
              (async/put! ch (:body response))))
  ch)

(defn fetch-data-periodically [ticker-symbol ch frequency]
  (async/go-loop []
    (fetch-data ticker-symbol ch)
    (async/<! (async/timeout (freq->ms frequency)))
    (recur))
  ch)


(defn line-to-bar [line]
  (update-in  (zipmap [:timestamp :close :high :low :open :volume]
                      (map read-string (s/split line #",")))
              [:timestamp] (fn [val] (java.util.Date. (* val 1000)))))

(defn time-transducer [start]
  (fn [step]
    (let [last-ts (atom start)]
      (fn
        ([] step)
        ([r] (step r))
        ([r x]
           (if (.after (:timestamp x) @last-ts)
             (do (reset! last-ts (:timestamp x))
                 (step r x))
             (step r)))))))

(def csv-to-bars-transducer
  (comp (mapcat s/split-lines)
        (filter #(Character/isDigit (first %)))
        (map line-to-bar)
        (time-transducer (java.util.Date. 115 0 1))
        ))

(defn process [symbol]
  (let [ch (async/chan 1 csv-to-bars-transducer)]
    (fetch-data-periodically symbol ch 1/60)
    ch))


(defn welcome! [app]
  (println "************************************")
  (println (format  "** %30s **" app))
  (println "************************************"))

(def load-config
  (comp read-string slurp))

(comment (defn get-system [config]
           (component/system-map
            :mq-connection (RabbitMQConnectionComponent.)
            :hearbeat (component/using (map->HeartbeatComponent {:service-name service-name
                                                                 :exchange "autrade.monitor"
                                                                 :frequency 1.0})
                                       [:mq-connection])
            :command-listener (component/using (map->CommandListenerComponent {:queue "mon"})
                                               [:mq-connection])
            :processor (component/using (map->ProcessorComponent {:mq-exchange "autrade.ticker"
                                                                  :mq-queue ""})
                                        [:mq-connection]))))

(def system nil)

(comment (defn -main [& args]
           (welcome! "Yahoo Feeder Service")
           (log/info "Starting Yahoo Feeder Service")

           (let [cfg-file (first args)
                 cfg-map (load-config cfg-file)
                 symbol (:symbol cfg-map)]

             (alter-var-root #'system (constantly (get-system cfg-map)))
             (alter-var-root #'system component/start))))

(defn -main [& args]
  (let [connection (rmq/connect {:automatically-recover true
                                 :automatically-recover-topology :true})
        channel (lch/open connection)
        monitoring-chan (asvc/publisher-chan (asvc/->LangohrPublisherEndpoint channel "autrade.monitor"))
        command-chan (asvc/subscriber-chan (asvc/->LangohrReceiverEndpoint channel "mon"))
        _ (heartbeat {:chan monitoring-chan :service-name service-name :frequency (freq->ms 1)})
        process-res-chan (process "RBI.VI")]))
