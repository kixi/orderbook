(ns orderbook.orderbook
  (:require [clojure.pprint]
            [orderbook.util :refer :all]
            [orderbook.types :refer :all])
  (:import [orderbook.types Order])
  (:gen-class))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(def ^:const buysell-set #{:buy :sell})

(def ^:const bid-buy-map {:buy :bid
                          :sell :ask})

(defrecord OrderIdx [^java.util.UUID order-id
                     buysell
                     ^double limit
                     ^java.util.Date enqueued]
  Comparable
  (compareTo [this other]
    (case buysell
      :buy (compare-or (compare (:limit other) limit)
                       (compare enqueued (:enqueued other))
                       (compare order-id (:order-id other)))
      :sell (compare-or (compare limit (:limit other) )
                        (compare enqueued (:enqueued other))
                        (compare order-id (:order-id other)))
      )))


(defn indexOrder [^Order order]
  (OrderIdx. (:order-id order) (:buysell order) (:limit order) (:enqueued order)))

(def ^:const empty-orderbook {:bid (sorted-map)
                              :ask (sorted-map)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Event processing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn update-orderbook [leg orderbook event]
  (let [order (assoc (:order event) :enqueued (:enqueued event))]
    (-> orderbook
        (update-in [leg]
                   assoc (indexOrder order) order))))

(defmulti apply-event (fn [orderbook event] (:event event)))

(defmethod apply-event :buy-order-placed [orderbook event]
  (update-orderbook :bid orderbook event))

(defmethod apply-event :sell-order-placed [orderbook event]
  (update-orderbook :ask orderbook event))

(defn enqueued-leg [event]
  (cond (get-in event [:orders :leg1 :enqueued]) :leg1
        (get-in event [:orders :leg2 :enqueued]) :leg2))

(defmethod apply-event :orders-matched [orderbook event]
  (let [idx-table (bid-buy-map (get-in event [:orders (enqueued-leg event) :buysell]))
        stored-order-idx (key (first (idx-table orderbook)))]
    (-> orderbook
        (update-in [idx-table]
                   (fn [ob-leg]
                     (if (:requeue-order event) 
                       (assoc ob-leg (indexOrder (:requeue-order event)) (:requeue-order event))
                       (dissoc ob-leg stored-order-idx)))))))

(defn apply-events [aggregate events]
  (reduce apply-event aggregate events))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; orderbook business logic
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn other-buysell-flag [buysell-flag]
  (case buysell-flag
    :buy :sell
    :sell :buy
    nil))

(defn matching-leg [orderbook buysell-flag]
  ((bid-buy-map (other-buysell-flag buysell-flag)) orderbook))

(defn match? [^Order order1 ^Order order2]
  (cond (not (and  order1  order2))
        false
        (and (= (:buysell order1) :buy)
             (= (:buysell order2) :sell))
        (>= (:limit order1) (:limit order2))
        (= (:buysell order1) (:buysell order2))
        false
        :else
        (recur order2 order1)))

(defn update-order-split-id [^Order order id]
  (update-in order [:split-id] #(str % "/" id)))

(defn split-order
  "creates two orders out of one. The first order has :quantity quantity
the second's quantity is reduced. returns a vector of the two orders: [order1 order2].
 if the order has 0 quantity nil is returned instead"
  [^Order order quantity]
  (if (= quantity (:quantity order))
    [order nil]
    [(update-order-split-id (assoc order :quantity quantity) 1)
     (update-order-split-id (update-in order [:quantity] #(- % quantity)) 2)]
    ))

(defn match-orders [^Order order1 ^Order order2]
  (when (match? order1 order2)
    (let [quantity (min (:quantity order1) (:quantity order2))
          [leg1 rem1] (split-order order1 quantity)
          [leg2 rem2] (split-order order2 quantity)]
      [{:leg1 leg1 :leg2 leg2}
       (or rem1 rem2)])))

(defn record-event [aggregate events evt f-now f-id-gen]
  (let [e (assoc evt :event-id (f-id-gen) :timestamp (f-now))]
    [(apply-event aggregate e) (conj events e)]))

(defn enqueue-order [orderbook ^Order order events f-now f-id-gen]
  (let [event-type (if (= :buy (:buysell order)) :buy-order-placed :sell-order-placed)]
    (record-event orderbook 
                  events
                  {:event event-type :order order :enqueued (f-now)}
                  f-now
                  f-id-gen)))

(declare place-order)

(defn make-order [orderbook ^Order order order-from-book events f-now f-id-gen]
  (let [[match-res remaining-order] (match-orders order order-from-book)
        [ob-new events-new] (record-event orderbook
                                          events
                                          {:event :orders-matched
                                           :orders match-res
                                           :requeue-order (when (:enqueued remaining-order) remaining-order)}
                                          f-now
                                          f-id-gen)] 
    (if (or (:enqueued remaining-order) (not remaining-order))
      [ob-new events-new]
      (place-order ob-new remaining-order events-new f-now f-id-gen))))

;;

(defn find-matching-order-candidate [orderbook order]
   (let [leg (matching-leg orderbook (:buysell order))
            entry (first leg)]
     (when entry (val entry)) ))

(defn place-order
  "places an order, matches, and eventuelley queues the order or part of it.
Returns [orderbook, [events]]"
  [orderbook ^Order order events f-now f-id-gen]
  (let [matching-order-candidate (find-matching-order-candidate orderbook order) ]

    (if-not (match? order matching-order-candidate)
      (enqueue-order orderbook order events f-now f-id-gen)
      (make-order orderbook order matching-order-candidate events f-now f-id-gen) )))

;;;;;;;;;;;;;;;

(defn create-place-order-f [f-now f-id-gen]
  (fn
    [orderbook ^Order order] ( place-order orderbook order [] f-now f-id-gen)))

(def place-order-m (create-place-order-f now! id-gen!) )


(defn handle [orderbook cmd]
  (place-order-m orderbook (map->Order (:order cmd))))


