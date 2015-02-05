(ns orderbook.util)



(defmacro compare-or
  ([] 0)
  ([f] f)
  ([f & next]
     `(let [compare# ~f]
        (if (= 0 compare#) (compare-or ~@next) compare#))))

(defn now! []
  (java.util.Date.))

(defn id-gen! []
  (java.util.UUID/randomUUID ))


(defmacro measure-time
  "Evaluates expr and returns the time"
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))
