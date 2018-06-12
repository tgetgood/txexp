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
   :next-fn    (fn [state input] nil)
   :flush      (fn [state] nil)})

(def my-scanning-transducer
  {:init-state {}
   :scan-fn    (fn [state last-val input] nil)
   :flush      (fn [state last-val] nil)})

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
    #_(transduce tx )))

(comment
  "This is needlessly complicated. Let's try simplifying again.")

(def my-transducer
  {:next-fn    (fn [current state input])
   :flush      (fn [current state])
   :init-state {}})

(def transduction
  {:transducer {}
   :state      {}
   :current {}})

(defn init-transduction [transducer]
  {:transducer transducer
   ;; REVIEW: Maybe we need an initial value as well.
   :current nil
   :state (:init-state transducer)})

(defn transduce [next-fn current state input]
  (let [step       (next-fn current state input)
        emissions  (cond
                     (contains? step :emit-n) (:emit-n step)
                     (contains? step :emit)   [(:emit step)]
                     :else                    [])
        next-state (if (contains? step :state) (:state step) state)]
    {:state           next-state
     :current   (if (seq emissions) (last emissions) current)
     :emissions emissions}))

(defn transduce-1 [transduction input]
  (let [{:keys [state current] {:keys [next-fn]} :transducer} transduction
        step (transduce next-fn current state input)]
    (merge transduction
           step
           {:emissions (concat (:emissions transduction) (:emissions step))})))

(defn compute [transduction inputs]
  (reduce transduce-1 transduction inputs))

(comment
  "And now we have transducers all over again from a different
  angle. Composition is no longer just basic function composition, but that's
  not important; we'll recover it eventually."

  "The key is that now that we have a reified current state of the transduction,
  we can pass it around as a value. This is process as a value, or as close as
  we can come to it."

  "So now let's try to deal with multi-transduce. Consequently we also now need
  to define interleaving."

  "I don't want to define interleaving. I also don't know how. The interleaving
  itself is business logic and not our concern. What we really need to define is
  stream tagging and merging: the act of combining multiple streams into one
  without losing track of the origin of messages.")

(def i1 (range 10 30))
(def i2 (range 20))

(defn get-tag [tagged-val]
  (first tagged-val))

(defn get-val [tagged-val]
  (second tagged-val))

(defn tag-stream [tag stream]
  (into (empty stream)
        (map (fn [x] [tag x]) stream)))

(defn tag-merge [interleave-fn & is]
  ;; Basically tag all values so that we know which stream they came from.
  (apply interleave-fn (map-indexed tag-stream is)))

(def multi-transducer
  {:init-state {}
   ;; This is a method table. It's written as a list because the methods are
   ;; "first arg", "second arg", etc..
   ;; Elsewhere I had named args. That introduces a layer of semantics on the
   ;; input names (if they're well named) but we need to add another layer of
   ;; indirection otherwise transducers can't be reused.
   :next-fn-multi [(fn [current state input]) (fn [current state input])]
   ;; Interesting question: when do we flush? When one input is exhausted? When
   ;; they all are? Do we need a list of flush fns?
   :flush (fn [current state])})

(defn get-tag-fn [tag transducer]
  (get-in transducer [:next-fn-multi tag]))

(comment
  "Transducer :: [Signal] -> Signal"

  "Transducers act the same way functions do in a classical FP system. Functions
  take data to data, but they don't do anything until data comes in in the first
  place. So you assemble a system by composing functions together into a call
  graph and then wait for input at runtime to do the actual computation."

  "What I'm here calling transducers are the same as functions in that
  respect. except that there is a much richer set of automatic transformations
  that can be performed by the compiler or the runtime to optimise or distribute
  the program."

  "In principle we should be able to preserve basic composition:"

  (t1 (t2 (t3 x y) z))

  "Is a transducing process, where x, y, & z are inputs from the outside world
  and the output is a signal to the outside world."

  "I'd really like to have a reasonable theory of process. That means effects
  are not only accounted for, but are somehow first class. They can't be allowed
  to destroy the system however.")


(defn multi-transduce-1 [{:keys [state current transducer] :as transducer} tagged-val]
  (let [step (transduce (get-tag-fn (get-tag tagged-val) transducer) current state (get-val tagged-val))]
    (merge transducer
           step
           ;; FIXME: This is very state-monad and not at all aesthetically appealing.
           {:emissions (concat (:emissions transducer) (:emissions step))})))

(defn multi-compute [transduction inputs]
  (reduce multi-transduce-1 transduction inputs))

(defn signal []
  (atom
   {:buffer []
    :subscribers []}))

(def my-transducer
  {:initial-value {}
   :init-state {}
   :next-fn [(fn [current state input])]
   ;; This revolves around what Bird calls left zeros. We're really cheating and
   ;; implicitly defining a predicate shortcut with (reduced value). Maybe we
   ;; should bring back the predicate.
   ;; If all inputs to a transducer have reached zeroes, we should terminate
   ;; early and save time, but just because one input is finished doesn't mean
   ;; we should ignore whatever else may come out of the others. That's up to
   ;; the business logic.
   :flush-fn (fn [current state])})

(def my-transduction
  {:transducer {}
   :current-value {}
   :state {}
   :in []
   :out []})

(defn interleave-all [& seqs]
  (loop [seqs seqs
         out []]
    (if (empty? seqs)
      out
      (let [s' (remove empty? seqs)
            l (reduce min Double/POSITIVE_INFINITY (map count s'))]
        (recur (map #(drop l %) s')
               (concat out (apply interleave s')))))))

(def signal
  ;; Subscribers is a list of downstream transducers.
  {:type ::sigsegv
   :subscribers []})

(defn build-transduction [transducer inputs]
  {:transducer transducer
   :state (:init-state transducer)
   :current-value (:init-value transducer)
   :in inputs
   :out })

(defmacro deftx [name {:keys [next-fn] :as transducer}]
  (let [nargs (if (fn? next-fn) 1 (count next-fn))
        arg-syms (take nargs (repeatedly (gensym)))]
    `(def ~name
       (fn [~@arg-syms]
         (build-transduction transducer [~@arg-syms])
         ;; Somehow we have to build a signal graph... The inputs need to "push
         ;; forward" into this transducer.
         ;;
         ;; These transducers are defined "backwards" in that they define
         ;; their inputs and emit to nowhere, whereas a usable physical
         ;; computational substrate needs to push information
         ;; forwards. Basically we're defining a flow: a computational graph.
         ;;
         ;; I think at this point signals need to be reified. They're basically
         ;; message queues that push into fns that push back into different
         ;; queues.
         ;;
         ;; In addition, since they're just queues, they can be pushed to from
         ;; the outside world and pulled from via anything. This lets us lauch
         ;; side effects in response to the values coming over a queue. DB
         ;; writes, HTTP requests, anything you can think of.
         ))))

(comment
  "I can't make any useful progress without some notion of a signal type. A
  signal passes chunks of value (vectors of length zero or more) from source
  (single) to (possibly multiple) subscribers."

  "Transducers don't care about who's listening to them, but the network has
  to.")

(defn handle-emission )
(defn emit [sig emission]
  (run!
   (fn [sub]
     (handle-emission sub (:type sig) emission))
   (:subscribers sig)))

(defn run-transduction [transduction]
  )
