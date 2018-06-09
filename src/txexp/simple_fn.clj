(ns txexp.simple-fn)

(defn emit [& vals]

  )

(defn emit-state [s & vals]
  )

(defn transducer
  [{:keys [init-state next-fn flush-fn]}]
  (fn [rf]
    (fn )))
