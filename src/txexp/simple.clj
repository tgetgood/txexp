(ns txexp.simple
  "Conceptually simplest helpers for creating transducers.")

(defn transducer
  [{:keys [init-state next-fn flush-fn]}]
  (fn [rf]
    (let [state (volatile! init-state)]
      (fn
        ([] (rf))
        ([acc]
         (if flush-fn
           (let [{:keys [emit emit-n]} (flush-fn @state)]
             (cond
               emit   (rf (unreduced (rf acc emit)))
               emit-n (rf (unreduced (reduce rf acc emit-n)))
               :else  (rf acc)))
           (rf acc)))
        ([acc next]
         (let [{:keys [emit emit-n] s :state} (next-fn @state next)]
           (when s
             (vreset! state s))
           (cond
             emit   (rf acc emit)
             emit-n (reduce rf acc emit-n)
             :else  acc)))))))

(comment
  "See how above we've abstracted away a lot of the bleeding abstractions that
  result from performance concerns. The creator of transducers really doesn't
  need to deal with identity, reduced, the reducing fn, or the accumulated
  result at all."

  "Of course this comes with a rather severe performance hit since we're boxing
  and unboxing on every element of the collection."

  "Performance shouldn't be a concern right now. Clarity and correctness should
  come first. Macros can recover the efficiency for us later on.")

(defn map* [f]
  (transducer
   {:next-fn (fn [_ x] {:emit (f x)})}))

(defn partition-all* [n]
  (transducer
   {:init-state []
    :next-fn    (fn [buffer next]
                  (let [buffer' (conj buffer next)]
                    (if (= (count buffer') n)
                      {:emit buffer'
                       :state []}
                      {:state buffer'})))
    :flush-fn   (fn [buffer]
                  {:emit buffer})}))

(defn map** [f]
  (transducer
   {:next-fn (fn [_ args] {:emit (apply f args)})})
  )
(comment
  "Now for the fun part. How can we transduce over multiple collections at
once?"

  (transduce (map +) * [1 2 3 4] [2 3 4 5])

  "Should be equivalent to"

  (reduce * (map + [1 2 3 4] [2 3 4 5]))

  "Should it not?"

  "With boxing this is easy"

  (transduce (map** +) * (partition 2 (interleave [1 2 3 4] [2 3 4 5])))

  "But can we do better? In particular, can we treat the two input sequences as
  fundamentally different things? A sort of stream polymorphism.")

(defn transducer* [opts]

  )

(defn map*2 [f]
  (transducer
   {:init-state nil
    :next-fns [(fn [_ i]
                 {:state i})
               (fn [i j]
                 {:state nil
                  :emit (f i j)})]}))

(comment
  "That's just weird. Why would you ever want to do things this way? Stick with
  me a minute, I'm not sure how to explain this... I'm also not sure how to do
  it.")
