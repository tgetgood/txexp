(ns txexp.simple-fn)

(defn emit [& vals]
  (fn [_ rf acc]
    (if (seq vals)
      (reduce rf acc vals)
      acc)))

(defn emit-state [s & vals]
  (fn [scb rf acc]
    (scb s) ; bloody effects
    (apply emit vals)))

(defn transducer
  [{:keys [init-state next-fn flush-fn]}]
  (fn [rf]
    (fn [])))
