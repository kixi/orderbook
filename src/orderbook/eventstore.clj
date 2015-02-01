(ns orderbook.eventstore
  (:require [clojure.core.async :as async]))


(defn run-eventstore! [event-ch command-ch]
  (async/go-loop []
    (let [evt (async/<! event-ch)]
      (if-let [ret-ch (:chan evt)]
        (async/>! ret-ch :success)))
    (recur))
  (async/go-loop []
    (let [cmd (async/<! command-ch)]
      (if-let [ret-ch (:chan cmd)]
        (async/close! ret-ch)))))
