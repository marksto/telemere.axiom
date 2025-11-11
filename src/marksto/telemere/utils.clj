;; Copyright (c) Mark Sto, 2025. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file `LICENSE` at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by the
;; terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns marksto.telemere.utils
  {:author "Mark Sto (@marksto)"})

(defn value+type
  "Returns a `{:value <obj>, :type (type <obj>)}` map for a given `obj`."
  [obj]
  {:value obj
   :type  (type obj)})

(defn interrupt!
  "Interrupts the current or the given `thread`.

   In the first case, i.e. when the current thread interrupts itself, this call
   effectively just sets the current thread's interrupt status and never throws
   itself.

   Returns `nil`."
  ([]
   (interrupt! (Thread/currentThread)))
  ([^Thread thread]
   (Thread/.interrupt thread)))

(defmacro uninterruptibly
  "Evaluates the `body`, retrying on `InterruptedException` until it completes
   successfully.

   Returns the result of the first successful evaluation. If any intermediate
   interrupts were seen, resets the current thread's interrupt status on exit."
  [& body]
  `(let [interrupted?# (volatile! false)
         sentinel-obj# (Object.)]
     (try
       (loop []
         (let [res# (try
                      (do ~@body)
                      (catch InterruptedException _#
                        ;; Remember that we were interrupted
                        ;; and proceed (with the loop logic)
                        (vreset! interrupted?# true)
                        sentinel-obj#))]
           ;; NB: Can only recur from tail position.
           (if (identical? res# sentinel-obj#)
             (recur)
             res#)))
       (finally
         ;; Restore a thread's interrupt status if necessary
         (when @interrupted?# (interrupt!))))))
