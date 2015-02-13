(ns orderbook.eventstore
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]))


(defn run-eventstore! [event-ch command-ch publish-ch]
  (async/thread
    (loop []
      (when-let [evt (async/<!! event-ch)]
        (log/debug "Eventstore: save event: " evt)
        (doseq [e (:events evt)]
          (async/>!! publish-ch (assoc e :aggregate-id (:aggregate-id evt))))

        (when-let [ret-ch (:chan evt)]
          (async/>!! ret-ch :success))
        (recur))))
  
  (async/thread
    (loop []
      (when-let [cmd (async/<!! command-ch)]
        (log/debug "Eventstore: command received" cmd)
        (if-let [ret-ch (:chan cmd)]
          (async/close! ret-ch))
        (recur)))))

(defn write! [chan]
  (let [writers (atom {})]
    (async/go-loop []
      (when-let [ evt (async/<! chan)]
        (swap! writers #(if (contains? % (:aggregate-id evt))
                          %
                          (assoc % (:aggregate-id evt) (aggregate-writer! "/tmp" (:aggregate-id evt)))))
        (if-let [ch (get @writers (:aggregate-id evt))]
          (async/>! ch (:events evt)))
        (recur)))
    (doseq [ch (vals @writers)]
      (async/close! ch))))


(defn aggregate-writer! [dir aggregate-id]
  (let [chan (async/chan)]
    (async/thread
      (log/debug "aggregate stream started " aggregate-id)
     (with-open [w (clojure.java.io/writer (str dir "/" aggregate-id) :append true)]
       (loop []
         (when-let [evt (async/<!! chan)]
           (.write w evt)
           (.newLine w)
           (.flush w)
           (recur)))
       )
      (log/debug "aggregate stream stopped " aggregate-id)
     )
    chan)
  )
