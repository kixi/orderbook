(ns orderbook.types
  (:require [orderbook.util :refer :all])
  (:gen-class))


(defrecord Order [^java.util.UUID order-id
                  buysell
                  ^double limit
                  ^long quantity
                  ^java.util.Date enqueued
                  ^String split-id
                  ])

(defn create-random-order []
  (let [bs (if (= ( rand-int 2) 0) :buy :sell)
        limit (if (= bs :buy) (+ 100 (rand 10)) (+ 100 (rand 10)))]
    (Order. (id-gen!)
            bs
            limit
            (+ 1 (rand-int 1000))
            nil
            nil)))
