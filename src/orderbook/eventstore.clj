(ns orderbook.eventstore
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]))


(defn run-eventstore! [event-ch command-ch publish-ch]
  (async/go-loop []
    (let [evt (async/<! event-ch)]
      (if-let [ret-ch (:chan evt)]
        (do (log/debug "Eventstore: save event: " evt)
            (async/>! publish-ch evt)
            (async/>! ret-ch :success))))
    (recur))
  
  (async/go-loop []
    (let [cmd (async/<! command-ch)]
      (log/debug "Eventstore: command received" cmd)
      (if-let [ret-ch (:chan cmd)]
        (async/close! ret-ch)))
    (recur)))
