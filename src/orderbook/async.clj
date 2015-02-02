(ns orderbook.async
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
            [com.stuartsierra.component :as component]))


(defprotocol PublisherEndpoint
  (publish-to-endpoint [endpoint msg]))

(defprotocol ReceiverEndpoint
  (subscribe-to-endpoint [endpoint handler]))

(defrecord LangohrPublisherEndpoint [channel exchange]
  PublisherEndpoint
  (publish-to-endpoint [endpoint msg]
    (lb/publish (:channel endpoint)
                (:exchange endpoint)
                ""
                (prn-str msg)
                {:content-type "text/plain" :type "edn"})))

(defrecord LangohrReceiverEndpoint [channel queue]
  ReceiverEndpoint
  (subscribe-to-endpoint [endpoint handler]
    (lc/subscribe (:channel endpoint)
                  (:queue endpoint)
                  (fn [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
                    (handler {:ch ch :meta meta :payload (read-string (String. payload "UTF-8"))}))
                  {:auto-ack true})))

(defn subscribe [endpoint handler]
  (subscribe-to-endpoint endpoint handler))

(defn publish [endpoint msg]
  (publish-to-endpoint endpoint msg))


(defn subscriber-chan
  ([endpoint]
     (let [ch (async/chan)]
       (subscriber-chan endpoint ch)))
  ([endpoint chan]
     (subscribe endpoint
                (fn [msg]
                  (async/>!! chan msg)))
     chan))

(defn publisher-chan
  ([endpoint]
     (let [ch (async/chan)]
       (publisher-chan endpoint ch)))
  ([endpoint chan]
     (async/go-loop []
       (publish endpoint (async/<! chan))
       (recur))
     chan)
  )
