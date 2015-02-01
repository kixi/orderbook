(ns orderbook.orderbook-svc
  ( :require [orderbook.orderbook :as dom]
             [orderbook.async-aggregate :as aggr]
             [clojure.core.async :as async]
             [clojure.tools.logging :as log]))

(defrecord Orderbook []
  aggr/Aggregate
  (initial-value [this]
    dom/empty-orderbook)
  (apply-event [this aggregate event]
    (dom/apply-event aggregate event))
  (handle [this aggregate command]
    (dom/handle aggregate command)))

(defn create-channels [orderbook-ids]
  (zipmap orderbook-ids (repeatedly #(async/chan 1000))))

(defn- build-distribution-channels! [cmd-ch aggregate-ids]
  (let [publisher (async/pub cmd-ch #(:product %))
        cmd-channels (create-channels aggregate-ids)]
    (doseq [id aggregate-ids]
      (async/sub publisher id (id cmd-channels)))
    cmd-channels))

(defn run-service! [cmd-ch event-ch products eventstore-cmd-ch]
  (log/info "Running orderbook services")
  (let [dist-channels (build-distribution-channels! cmd-ch products)
        startup-channels (create-channels products)]
    (doseq [id products]
      (aggr/run-aggregate! (Orderbook.) id (id dist-channels) event-ch (id startup-channels))
      (async/put! eventstore-cmd-ch {:load-events id :chan (id startup-channels)}))))
