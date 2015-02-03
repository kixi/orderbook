(ns orderbook.core-test
  (:require [clojure.test :refer :all]
            [orderbook.core :refer :all]
            [orderbook.async :as asvc]
            [orderbook.orderbook-svc :as svc]
            [clojure.core.async :as async]
            [orderbook.eventstore :as es]))

(deftest integration-test-wo-queues
  (testing "scenarios"
    (let [cmd-ch (async/chan)
          es-save-ch (async/chan)
          es-cmd-ch (async/chan)
          evt-ch (async/chan)]

      (es/run-eventstore! es-save-ch es-cmd-ch evt-ch)
      (svc/run-service! cmd-ch es-save-ch [:USD :CHF :GBP] es-cmd-ch)

      (async/put! cmd-ch {:product :USD :order {:order-id "1" :limit 1.2 :buysell :buy :quantity 10}})
      (is (= 1 (async/<!! evt-ch)))

      (async/put! cmd-ch {:product :USD :order {:order-id "2" :limit 1.21 :buysell :buy :quantity 10}})
      (is (= 1 (async/<!! evt-ch)))

      (async/put! cmd-ch {:product :USD :order {:order-id "3" :limit 1.22 :buysell :sell :quantity 10}})
      (is (= 1 (async/<!! evt-ch)))

      (async/put! cmd-ch {:product :USD :order {:order-id "4" :limit 1.21 :buysell :sell :quantity 10}})
      (is (= 1 (async/<!! evt-ch)))

      (async/put! cmd-ch {:product :USD :order {:order-id "5" :limit 1.22 :buysell :buy :quantity 10}})
      (is (= 1 (async/<!! evt-ch)))

      (async/put! cmd-ch {:product :USD :order {:order-id "5" :limit 1.2 :buysell :sell :quantity 10}})
      (is (= 1 (async/<!! evt-ch)))

      )))
