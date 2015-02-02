(ns orderbook.orderbook-test
  (:require [clojure.test :refer :all]
            [orderbook.orderbook :refer :all]))
(def date1 (java.util.Date.))
(def date2 (java.util.Date. (+ 1 (.getTime date1))))

(defn fake-now []
  (let [x (atom 0)]
    (fn [] (swap! x inc))))

(def fake-id-gen fake-now)

(defn place-order-test [orderbook order]
  ((create-place-order-f (fake-now) (fake-id-gen)) orderbook (map->Order (update (assoc order :order-id "1") :limit * 1.0))))

(defn ask-compare [order-idx1 order-idx2]
  (compare (map->OrderIdx (assoc order-idx1 :buysell :sell :order-id "1"))
           (map->OrderIdx (assoc order-idx2 :buysell :sell :order-id "1" ))))

(defn bid-compare [order-idx1 order-idx2]
  (compare (map->OrderIdx (assoc order-idx1 :buysell :buy :order-id "1"))
           (map->OrderIdx (assoc order-idx2 :buysell :buy :order-id "1" ))))

(deftest a-test
  (testing "Testing primitives"
    (is (= (ask-compare {:limit 100.0 :enqueued date1} {:limit 100.0 :enqueued date1}) 0))
    (is (= (ask-compare {:limit 101.0 :enqueued date1} {:limit 100.0 :enqueued date1}) 1))
    (is (= (ask-compare {:limit 100.0 :enqueued date2} {:limit 100.0 :enqueued date1}) 1))
    (is (= (ask-compare {:limit 100.0 :enqueued date1} {:limit 101.0 :enqueued date1}) -1))
    (is (= (ask-compare {:limit 100.0 :enqueued date1} {:limit 100.0 :enqueued date2}) -1))
    (is (= (bid-compare {:limit 100.0 :enqueued date1} {:limit 100.0 :enqueued date1}) 0))
    (is (= (bid-compare {:limit 101.0 :enqueued date1} {:limit 100.0 :enqueued date1}) -1))
    (is (= (bid-compare {:limit 100.0 :enqueued date2} {:limit 100.0 :enqueued date1}) 1))
    (is (= (bid-compare {:limit 100.0 :enqueued date1} {:limit 101.0 :enqueued date1}) 1))
    (is (= (bid-compare {:limit 100.0 :enqueued date1} {:limit 100.0 :enqueued date2}) -1))
    (is (= empty-orderbook {:bid {} :ask {}}))
    (is (= (other-buysell-flag :buy) :sell))
    (is (= (other-buysell-flag :sell) :buy))
    (is (= (other-buysell-flag :unknown) nil))
    (is (= (matching-leg {:bid "bid" :ask "ask"} :buy) "ask"))
    (is (= (matching-leg {:bid "bid" :ask "ask"} :sell) "bid"))
    (is (= (match? nil nil) false))
    (is (= (match? {:buysell :buy :limit 100} {:buysell :sell :limit 99} ) true))
    (is (= (match? {:buysell :buy :limit 100} {:buysell :sell :limit 100} ) true))
    (is (= (match? {:buysell :buy :limit 100} {:buysell :sell :limit 101} ) false))
    (is (= (match? {:buysell :sell :limit 100} {:buysell :buy :limit 99} ) false))
    (is (= (match? {:buysell :sell :limit 100} {:buysell :buy :limit 100} ) true))
    (is (= (match? {:buysell :sell :limit 100} {:buysell :buy :limit 101} ) true))
    (is (= (split-order {:quantity 10} 5) [ {:quantity 5 :split-id "/1"}
                                            {:quantity 5 :split-id "/2"}]))
    (is (= (split-order {:quantity 10} 10) [{ :quantity 10}   nil ]))
    (is (= (split-order {:quantity 5} 10) [ {:quantity 10 :split-id "/1"}
                                            {:quantity -5 :split-id "/2"}]))
    (is (= (match-orders {:buysell :sell :limit 100 :quantity 1}
                         {:buysell :buy :limit 101 :quantity 1})
           [ {:leg1 {:buysell :sell :limit 100 :quantity 1}
              :leg2 {:buysell :buy :limit 101 :quantity 1} } nil]))
    (is (= (match-orders {:buysell :sell :limit 100 :quantity 20}
                         {:buysell :buy :limit 101 :quantity 1})
           [ {:leg1 {:buysell :sell :limit 100 :quantity 1 :split-id "/1"}
              :leg2 {:buysell :buy :limit 101 :quantity 1} }
             {:buysell :sell :limit 100 :quantity 19 :split-id "/2"}]))
    (is (= (match-orders {:buysell :sell :limit 100 :quantity 1}
                         {:buysell :buy :limit 101 :quantity 20})
           [ {:leg1 {:buysell :sell :limit 100 :quantity 1}
              :leg2 {:buysell :buy :limit 101 :quantity 1 :split-id "/1"} }
             {:buysell :buy :limit 101 :quantity 19 :split-id "/2"}]))
    
    )


  (comment (t empty-orderbook
              {:given []
               :when ['place-order-test {:buysell :buy :limit 100 :quantity 1}]
               :then  [{:timestamp 2,
                        :event-id 1,
                        :event :buy-order-placed,
                        :order
                        (map->Order {:order-id "1",
                                     :buysell :buy,
                                     :limit 100.0,
                                     :quantity 1,
                                     :enqueued nil,
                                     :split-id nil}),
                        :enqueued 1}]}))
  (testing "Orderbook event logic"
    (let [[ob events] (place-order-test empty-orderbook {:buysell :buy :limit 100 :quantity 1})]
      (is (= events [{:timestamp 2,
              :event-id 1,
              :event :buy-order-placed,
              :order
                      (map->Order {:order-id "1",
                                   :buysell :buy,
                                   :limit 100.0,
                                   :quantity 1,
                                   :enqueued nil,
                                   :split-id nil}),
              :enqueued 1}]))))
  (comment 
    ( testing "Orderbook same leg"
      (is (= (place-order-test {:bid (bid-set { :buysell :buy :limit 100 :quantity 1 :enqueued 1}) :ask {}}
                               {:buysell :buy :limit 101 :quantity 1})
             [{:bid {{ :buysell :buy :limit 101 :quantity 1 :enqueued 1} {}
                     { :buysell :buy :limit 100 :quantity 1 :enqueued 1}  {}}
               :ask {}}
              [{:timestamp 2,
                :event-id 1,
                :event :order-placed,
                :order {:limit 101, :buysell :buy, :quantity 1},
                :enqueued 1}]
              ])))
    (testing "Orderbook no match"
      (is (= (place-order-test {:bid (bid-set {:buysell :buy :limit 100 :quantity 1 :enqueued 1})
                                :ask (ask-set { :buysell :sell :limit 102 :quantity 1 :enqueued 1})}
                               {:buysell :buy :limit 101 :quantity 1})
             [{:bid {{ :buysell :buy :limit 101 :quantity 1 :enqueued 1} {}
                     { :buysell :buy :limit 100 :quantity 1 :enqueued 1} {} }
               :ask {{ :buysell :sell :limit 102 :quantity 1 :enqueued 1} {}}}
              [{:timestamp 2,
                :event-id 1,
                :event :buy-order-placed,
                :order {:limit 101, :buysell :buy, :quantity 1},
                :enqueued 1}]
              ]))
      (is (= (place-order-test {:bid (bid-set { :buysell :buy :limit 100 :quantity 1 :enqueued 1})
                                :ask (ask-set { :buysell :sell :limit 102 :quantity 1 :enqueued 1})}
                               {:buysell :sell :limit 101 :quantity 1})
             [{:bid {{ :buysell :buy :limit 100 :quantity 1 :enqueued 1}
                     {}}
               :ask {{ :buysell :sell :limit 102 :quantity 1 :enqueued 1} {} 
                     { :buysell :sell :limit 101 :quantity 1 :enqueued 1}  {}}}
              [{:timestamp 2,
                :event-id 1,
                :event :sell-order-placed,
                :order {:limit 101, :buysell :sell, :quantity 1},
                :enqueued 1}]
              ])))
    (testing "Orderbook match"
      (is (= (place-order-test {:bid (bid-set { :buysell :buy :limit 100 :quantity 1 :enqueued 1})
                                :ask (ask-set { :buysell :sell :limit 102 :quantity 1 :enqueued 1})}
                               {:buysell :buy :limit 102 :quantity 1})
             [{:bid { {:buysell :buy :limit 100 :quantity 1 :enqueued 1}  {}}
               :ask {}}
              [{:timestamp 1,
                :event-id 1,
                :event :orders-matched,
                :orders
                {:leg1 {:limit 102, :buysell :buy, :quantity 1},
                 :leg2 {:limit 102, :enqueued 1, :buysell :sell, :quantity 1}} 
                :requeue-order nil}]
              ]))
      (is (= (place-order-test {:bid (bid-set { :buysell :buy :limit 108 :quantity 2 :enqueued 1}
                                              { :buysell :buy :limit 107 :quantity 2 :enqueued 1}
                                              { :buysell :buy :limit 106 :quantity 2 :enqueued 1}
                                              { :buysell :buy :limit 105 :quantity 2 :enqueued 1}
                                              { :buysell :buy :limit 104 :quantity 2 :enqueued 1}
                                              ) 
                                :ask (ask-set { :buysell :sell :limit 110 :quantity 1 :enqueued 1})}
                               {:buysell :sell :limit 104 :quantity 9})
             [{:bid
               {{:split-id "/2",
                 :limit 104,
                 :enqueued 1,
                 :buysell :buy,
                 :quantity 1} {}},
               :ask {{:limit 110, :enqueued 1, :buysell :sell, :quantity 1} {}}}
              [{:timestamp 1,
                :event-id 1,
                :event :orders-matched,
                :orders
                {:leg1 {:split-id "/1", :limit 104, :buysell :sell, :quantity 2},
                 :leg2 {:limit 108, :enqueued 1, :buysell :buy, :quantity 2}},
                :requeue-order nil}
               {:timestamp 2,
                :event-id 2,
                :event :orders-matched,
                :orders
                {:leg1
                 {:split-id "/2/1", :limit 104, :buysell :sell, :quantity 2},
                 :leg2 {:limit 107, :enqueued 1, :buysell :buy, :quantity 2}},
                :requeue-order nil}
               {:timestamp 3,
                :event-id 3,
                :event :orders-matched,
                :orders
                {:leg1
                 {:split-id "/2/2/1", :limit 104, :buysell :sell, :quantity 2},
                 :leg2 {:limit 106, :enqueued 1, :buysell :buy, :quantity 2}},
                :requeue-order nil}
               {:timestamp 4,
                :event-id 4,
                :event :orders-matched,
                :orders
                {:leg1
                 {:split-id "/2/2/2/1", :limit 104, :buysell :sell, :quantity 2},
                 :leg2 {:limit 105, :enqueued 1, :buysell :buy, :quantity 2}},
                :requeue-order nil}
               {:timestamp 5,
                :event-id 5,
                :event :orders-matched,
                :orders
                {:leg1
                 {:split-id "/2/2/2/2", :limit 104, :buysell :sell, :quantity 1},
                 :leg2
                 {:split-id "/1",
                  :limit 104,
                  :enqueued 1,
                  :buysell :buy,
                  :quantity 1}},
                :requeue-order
                {:split-id "/2",
                 :limit 104,
                 :enqueued 1,
                 :buysell :buy,
                 :quantity 1}}]]))
      )
    ))
