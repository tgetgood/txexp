(ns txexp.simple
  "Conceptually simplest helpers for creating transducers.")

(defn last-rf
  "Reducing fn that acts like last on a sequential collection"
  ([] nil)
  ([final] final)
  ([_ next] next))

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
  come first. Macros can recover the efficiency for us later on."

  "Maybe I should move sideways to show how macros can be used to make this as
  fast as the low level API. We're basically using macroexpansion as an
  optimising compiler.")

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

(comment "Optimising macro. It's going to have unfortunate corner cases.")

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
  it."

  )

(comment
  "I think we need to step back a second and reframe the discussion. Reduction
  hides the internal state until the very end, whereas what we really want is
  accumulation. That is, we want to take each element of the input sequence and
  generate the reduction so far. That value can then be passed back in with the
  tail of the sequence to get the next value, and so on. scanl in Haskell
  speak,"

  "Thinking of the reduction as the last value of an accumulation is akin to a
  cps transform. It allows us to pass the partially accumulated result back and
  forth between different reducing functions with values from different
  sequences.")

(def my-transducer
  {:init-state {}
   :next-fn (fn [state input] nil)
   :flush (fn [state] nil)})

(def my-scanning-transducer
  {:init-state {}
   :scan-fn (fn [state last-val input] nil)
   :flush (fn [state last-val] nil)})

(comment
  "Any reducer can be recovered from a scanning transducer via"

  (transduce scanning-transducer last-rf init input)

  "But since scanning transducers contain transduction state, they're capable of
  doing more."

  "Is this a good thing? I don't know. Maybe they shouldn't have state."

  "A transduction in progress looks like:")

(defn transduce-1 [{:keys [state current-val] {:keys [next-fn scan-fn]} :transducer :as txn} input]
  (let [out (if next-fn
              (next-fn state input)
              (scan-fn state current-val input))]
    ;; We've lost the ability to deal with multiple emissions. There's no mechanism here to model that.
    ;; So I think we need types for transducers and types for messages. Nothing, single, multiple.
    ;; Wait. In principle this is just a list. Empty, one thing, more
    ;; things. Why bother with types at first.
    (assoc txn :state (if (:state out) (:state out) state)
           :emission (cond
                       (contains? out :emit-n) (:emit-n out)
                       (contains? out :emit)   [(:emit out)]
                       :else                   [])
           :current-val (cond
                          (:emit out)   (:emit out)
                          (:emit-n out) (last (:emit-n out))
                          :else         current-val))))

(def transduction
  {:current-val {}
   :state {}
   :transducer {}})

(defn compute [tx coll]
  (transduce (transducer tx) last-rf coll))

(defn advance
  "Advance a computation by one step"
  [transduction input]
  (let [{:keys [current-val state transducer]} transduction]
    (if (:next-fn transducer)
      ((:next-fn transducer) state input)
      ((:scan-fn transducer) state current-val input))
    (transduce tx )))
