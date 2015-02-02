(ns orderbook.async-aggregate-test
  (:require [clojure.test :refer :all]
            [orderbook.async-aggregate :refer :all]
            [clojure.core.async :as async]))


(def dummy-aggregate
  (partial run-aggregate!
           (reify Aggregate
             (initial-value [this] 0)
             (apply-event [this aggr event]
               (:val event))
             (handle [this aggr cmd]
               (let [e {:event-type :incremented :val (+ aggr 1)}]
                 [(apply-event this aggr e) [e]])))))


(deftest first-test
  (testing "restore aggregate"
    (let [cmd-ch (async/chan)
          startup-ch (async/chan)
          event-ch (async/chan)
          aggr (dummy-aggregate "1" cmd-ch event-ch startup-ch)]
      (async/put! startup-ch {:val 5})
      (async/close! startup-ch)
      (async/put! cmd-ch {:cmd :increment})
      (let [event (async/<!! event-ch)]
        (is (= (dissoc event :chan)  {:aggregate-id "1"
                                      :events [{:event-type :incremented :val 6}]}))
        (async/put! (:chan event) :success)
        (async/close! cmd-ch))))
  
  (testing "empty aggregate"
    (let [cmd-ch (async/chan)
          startup-ch (async/chan)
          event-ch (async/chan)
          aggr (dummy-aggregate "1" cmd-ch event-ch startup-ch)]
      (async/close! startup-ch)
      (async/put! cmd-ch {:cmd :increment})
      (let [event (async/<!! event-ch)]
        (is (= (dissoc event :chan)  {:aggregate-id "1"
                                      :events [{:event-type :incremented :val 1}]}))
        (async/put! (:chan event) :success)
        (async/close! cmd-ch))))) 
