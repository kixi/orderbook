(ns orderbook.async-aggregate
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]))


(defprotocol Aggregate
  (initial-value [this])
  (apply-event [this aggregate event])
  (handle [this aggregate command]))


(defn run-aggregate! [aggr aggregate-id cmd-ch event-ch startup-ch]
  (let [orderbook (atom (initial-value aggr))]
    (log/info "Starting aggregate " aggregate-id)
    ;; restore aggregate
    ;; restore will be finished as soon as the startup-ch channel is closed

    (async/go
      (log/info "Restoring aggregate " aggregate-id)
      (loop []
        (when-let [e (async/<! startup-ch)]
          (swap! orderbook (partial apply-event aggr) e)
          (recur)))

      ;; cmd handler
      (log/info "Command Handler started " aggregate-id)
      (loop []
        (when-let [cmd (async/<! cmd-ch) ]
          (let [_ (log/debug "Command received " cmd)
                ;; process command
                [ob events] (handle aggr @orderbook cmd)
                ret-chan (async/chan)]

            (log/debug "Store results " ob events)
            ;; store events
            (async/>! event-ch {:aggregate-id aggregate-id
                                :chan ret-chan
                                :events events})

            (log/debug "Waiting for response")
            (case (async/<! ret-chan)
              ;; update aggregate
              :success (reset! orderbook ob))
            (recur))))
      (log/info "Command Handler closed " aggregate-id))))
