(ns orderbook.core-test
  (:require [clojure.test :refer :all]
            [orderbook.core :refer :all]
            [orderbook.async :as asvc]
            [orderbook.orderbook-svc :as svc]
            [clojure.core.async :as async]
            [orderbook.eventstore :as es]
            [orderbook.util :refer :all]))

(defn clean-to-compare [e]
  (-> e
      (dissoc :enqueued :timestamp :event-id)
      (update-in [:order] into {})
      (update-in [:order] dissoc :enqueued :split-id)))

(defn clean-to-compare-legs [e]
  (-> e
      (dissoc :enqueued :timestamp :event-id :requeue-order)
      (update-in [:orders :leg1 ] into {})
      (update-in [:orders :leg2 ] into {})
      (update-in [:orders :leg1] dissoc :enqueued :split-id)
      (update-in [:orders :leg2] dissoc :enqueued :split-id)))

(deftest integration-test-wo-queues
  (testing "scenarios"
    (let [cmd-ch (async/chan)
          es-save-ch (async/chan)
          es-cmd-ch (async/chan)
          evt-ch (async/chan)]

      (es/run-eventstore! es-save-ch es-cmd-ch evt-ch)
      (svc/run-service! cmd-ch es-save-ch [:USD :CHF :GBP] es-cmd-ch)

      (async/put! cmd-ch {:product :USD :order {:order-id "1" :limit 1.2 :buysell :buy :quantity 10}})
      (is (= {:aggregate-id :USD :event :buy-order-placed, :order {:order-id "1", :buysell :buy, :limit 1.2, :quantity 10}}
             (clean-to-compare (async/<!! evt-ch))))

      (async/put! cmd-ch {:product :USD :order {:order-id "2" :limit 1.21 :buysell :buy :quantity 10}})
      (is (= {:aggregate-id :USD :event :buy-order-placed, :order {:order-id "2", :buysell :buy, :limit 1.21, :quantity 10}}
             (clean-to-compare (async/<!! evt-ch))))

      (async/put! cmd-ch {:product :USD :order {:order-id "3" :limit 1.22 :buysell :sell :quantity 10}})
      (is (= {:aggregate-id :USD :event :sell-order-placed, :order {:order-id "3", :buysell :sell, :limit 1.22, :quantity 10}}
             (clean-to-compare (async/<!! evt-ch))))

      (async/put! cmd-ch {:product :USD :order {:order-id "4" :limit 1.21 :buysell :sell :quantity 10}})
      (is (= {:aggregate-id :USD :event :orders-matched,
              :orders {:leg1 {:order-id "4", :buysell :sell, :limit 1.21, :quantity 10}
                       :leg2 {:order-id "2", :buysell :buy, :limit 1.21, :quantity 10}}}
             (clean-to-compare-legs (async/<!! evt-ch))))

      (async/put! cmd-ch {:product :CHF :order {:order-id "41" :limit 1.21 :buysell :sell :quantity 10}})
      (is (= {:aggregate-id :CHF :event :sell-order-placed, :order {:order-id "41", :buysell :sell, :limit 1.21, :quantity 10}}
             (clean-to-compare (async/<!! evt-ch))))

      (async/put! cmd-ch {:product :USD :order {:order-id "5" :limit 1.22 :buysell :buy :quantity 10}})
      (is (= {:aggregate-id :USD :event :orders-matched,
              :orders {:leg1 {:order-id "5", :buysell :buy, :limit 1.22, :quantity 10}
                       :leg2 {:order-id "3", :buysell :sell, :limit 1.22, :quantity 10}}}
             (clean-to-compare-legs (async/<!! evt-ch))))

      (async/put! cmd-ch {:product :USD :order {:order-id "6" :limit 1.2 :buysell :sell :quantity 10}})
      (is (= {:aggregate-id :USD :event :orders-matched,
              :orders {:leg1 {:order-id "6", :buysell :sell, :limit 1.2, :quantity 10}
                       :leg2 {:order-id "1", :buysell :buy, :limit 1.2, :quantity 10}}}
             (clean-to-compare-legs (async/<!! evt-ch))))
      
      (async/put! cmd-ch {:product :CHF :order {:order-id "42" :limit 1.21 :buysell :buy :quantity 10}})
      (is (= {:aggregate-id :CHF :event :orders-matched,
              :orders {:leg1 {:order-id "42", :buysell :buy, :limit 1.21, :quantity 10}
                       :leg2 {:order-id "41", :buysell :sell, :limit 1.21, :quantity 10}}}
             (clean-to-compare-legs (async/<!! evt-ch))))

      )))

(defn random-order []
  {:product (rand-nth [:CHF :GBP :USD] )
   :order {:order-id (java.util.UUID/randomUUID)
           :limit (+ 1.0 (* 0.05 (rand)))
           :quantity (+ 1 (rand-int 1000))
           :buysell (rand-nth [:buy :sell])}})

(defn place-orders!! [cnt cmd-ch evt-ch]
                (async/go
                  (loop [x cnt]
                    (when-not (= 0 x)
                      (async/>! cmd-ch (random-order))
                      (recur (dec x)))))
                
                (async/<!!
                 (async/go
                   (loop [x 0]
                     (if (< x cnt)
                       (do
                         (async/<! evt-ch)
                         (recur (inc x)))
                       ))
                   :finished)))

(deftest performance-test-wo-queues
  (testing "scenarios"
    (let [cmd-ch (async/chan 10000)
          es-save-ch (async/chan 10000)
          es-cmd-ch (async/chan)
          evt-ch (async/chan 10000)]

      (es/run-eventstore! es-save-ch es-cmd-ch evt-ch)
      (svc/run-service! cmd-ch es-save-ch [:USD :CHF :GBP] es-cmd-ch)

      (place-orders!! 100000 cmd-ch evt-ch)
      (is (< (measure-time (place-orders!! 1000000 cmd-ch evt-ch)) 1000)))))
