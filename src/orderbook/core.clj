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
  (let [connection (rmq/connect {:automatically-recover true
                                 :automatically-recover-topology :true})
        channel (lch/open connection)
        monitoring-chan (asvc/publisher-chan (asvc/->LangohrPublisherEndpoint channel "autrade.monitor"))
        command-chan (asvc/subscriber-chan (asvc/->LangohrReceiverEndpoint channel "mon"))
        _ (heartbeat {:chan monitoring-chan :service-name service-name :frequency (freq->ms 1)})
]))
