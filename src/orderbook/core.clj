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
            [orderbook.async :as asvc]
            [orderbook.orderbook-svc :as svc]
            [orderbook.eventstore :as es]))

(def service-name "OrderbookService")

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


(defn -main [& args]
  (let [mq-connection (rmq/connect {:automatically-recover true
                                    :automatically-recover-topology :true})
        mq-channel (lch/open mq-connection)
        monitoring-chan (asvc/publisher-chan (asvc/->LangohrPublisherEndpoint mq-channel "orderbook.monitor"))
        command-chan (asvc/subscriber-chan (asvc/->LangohrReceiverEndpoint mq-channel "orderbook.command") (async/chan 10000 (map #(:payload %))))
        event-chan (asvc/publisher-chan (asvc/->LangohrPublisherEndpoint mq-channel "orderbook.events") (async/chan 10000))

        eventstore-save-ch (async/chan 10000)
        eventstore-cmd-ch (async/chan 10000)
        
        _ (comment (heartbeat {:chan monitoring-chan :service-name service-name :frequency (freq->ms 1)}))

        _ (es/run-eventstore! eventstore-save-ch eventstore-cmd-ch event-chan)
        _ (svc/run-service! command-chan eventstore-save-ch [:USD :CHF :GBP] eventstore-cmd-ch)
        
        ]))


(defn mq-client-channel []
  (let [mq-connection (rmq/connect {:automatically-recover true
                                    :automatically-recover-topology :true})
        mq-channel (lch/open mq-connection)
        command-chan (asvc/publisher-chan (asvc/->LangohrPublisherEndpoint mq-channel "orderbook.command.exchange"))

        
        
        ]
    command-chan))

(defn random-orders [ch cnt]
  (doseq [x (range cnt)]
    (async/>!! ch {:product :USD :order {:order-id (str x) :limit (rand) :quantity 10 :buysell (if (= (rand-int 2) 0) :buy :sell)}})))
