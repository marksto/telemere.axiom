(ns marksto.telemere.axiom-test
  (:require [clj-http.fake :refer [with-global-fake-routes-in-isolation]]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [jsonista.core :as json]
            [marksto.telemere.axiom :as sut]
            [matcher-combinators.test]
            [taoensso.telemere :as tel])
  (:import (clojure.lang ExceptionInfo)
           (java.time Instant)))

(def fake-api-url:success
  "https://successful.axiom.co")

(defn fake-api-url:failure
  ([]
   (fake-api-url:failure 400))
  ([status-code]
   (str "https://error" status-code ".axiom.co")))

(def fake-api-url:success-re #"^https://successful.axiom.co.+")
(def fake-api-url:failure-re #"^https://error(\d+).axiom.co.+")

(defn response:success
  [signals]
  {:status  200
   :headers {"Content-Type"              "application/json"
             "X-Content-Type-Options"    "nosniff"
             "X-Ingestlimit-Limit"       "536870912000"
             "X-Ingestlimit-Remaining"   "536864132134"
             "X-Ingestlimit-Reset"       "1749305461"
             "Strict-Transport-Security" "max-age=31536000; includeSubDomains; preload"
             "Connection"                "close"
             "Pragma"                    "no-cache"
             "Transfer-Encoding"         "chunked"
             "Date"                      "Sat, 07 Jun 2025 14:06:38 GMT"
             "Vary"                      "Origin"
             "Cache-Control"             "no-cache, no-store, private, max-age=0"}
   :body    (json/write-value-as-string
              {:walLength      (count signals)
               :blocksCreated  0
               :ingested       (count signals)
               :failures       []
               :failed         0
               :processedBytes (rand-int 1000)}
              sut/default-obj-mapper)})

(defn response:failure
  [status-code]
  {:status  status-code
   :headers {"Content-Type"              "application/json"
             "X-Content-Type-Options"    "nosniff"
             "Content-Length"            "35"
             "Strict-Transport-Security" "max-age=31536000; includeSubDomains; preload"
             "Connection"                "close"
             "Pragma"                    "no-cache"
             "Transfer-Encoding"         "chunked"
             "Date"                      "Sat, 07 Jun 2025 14:06:38 GMT"
             "Vary"                      "Origin"
             "Cache-Control"             "no-cache, no-store, private, max-age=0"}
   :body    (json/write-value-as-string
              {:code    status-code
               :message "Fake error"}
              sut/default-obj-mapper)})

(defonce *test-state (atom nil))

(def test-state:none {:requests {:succeeded []
                                 :failed    []}
                      :batches  []})

(defn update-test-state!
  [request successful?]
  (swap! *test-state
         (fn [test-state]
           (-> test-state
               (update-in [:requests (if successful? :succeeded :failed)]
                          conj request)
               (update :batches conj (:body request))))))

(defn parse-json [obj]
  (json/read-value obj sut/default-obj-mapper))

(defn respond
  [successful? {:keys [url] :as request}]
  (let [request' (update request :body parse-json)]
    (update-test-state! request' successful?)
    (if successful?
      (response:success (:body request'))
      (let [[_ status-code] (re-find fake-api-url:failure-re url)]
        (response:failure status-code)))))

(defn with-fake-http
  [f]
  (reset! *test-state test-state:none)
  (with-global-fake-routes-in-isolation
    {fake-api-url:success-re #(respond true %)
     fake-api-url:failure-re #(respond false %)}
    (f)))

(use-fixtures :each with-fake-http)

(def test-api-token (str "xaat-" (random-uuid)))
(def test-dataset "test-dataset")

(def test-signal-matcher
  {:ns    "marksto.telemere.axiom"
   :msg   "Axiom handler is added"
   :level "info"
   :kind  "log"})

(deftest validate-constructor-opts!-test
  (testing "`:api-token` fails validation"
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:conn-opts :api-token` string"
          (sut/validate-constructor-opts! {:conn-opts nil})))
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:conn-opts :api-token` string"
          (sut/validate-constructor-opts! {:conn-opts {:api-token nil}}))))
  (testing "`:dataset` fails validation"
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:conn-opts :dataset` string"
          (sut/validate-constructor-opts! {:conn-opts {:api-token test-api-token}})))
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:conn-opts :dataset` string"
          (sut/validate-constructor-opts! {:conn-opts {:api-token test-api-token
                                                       :dataset   nil}}))))
  (testing "`:batch-size` fails validation"
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:batch-size` positive integer"
          (sut/validate-constructor-opts! {:conn-opts  {:api-token test-api-token
                                                        :dataset   test-dataset}
                                           :batch-size nil})))
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:batch-size` positive integer"
          (sut/validate-constructor-opts! {:conn-opts  {:api-token test-api-token
                                                        :dataset   test-dataset}
                                           :batch-size -1}))))
  (testing "`:period-ms` fails validation"
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:period-ms` positive integer"
          (sut/validate-constructor-opts! {:conn-opts {:api-token test-api-token
                                                       :dataset   test-dataset}
                                           :period-ms nil})))
    (is (thrown-with-msg?
          ExceptionInfo
          #"Expected `:period-ms` positive integer"
          (sut/validate-constructor-opts! {:conn-opts {:api-token test-api-token
                                                       :dataset   test-dataset}
                                           :period-ms -1}))))
  (testing "successful validation"
    (is (nil? (sut/validate-constructor-opts! {:conn-opts {:api-token test-api-token
                                                           :dataset   test-dataset}})))
    (is (nil? (sut/validate-constructor-opts! {:conn-opts  {:api-token test-api-token
                                                            :dataset   test-dataset}
                                               :batch-size 1000
                                               :period-ms  250})))))

(deftest default-prepare-fn-test
  (testing "preparing a signal (default impl)"
    (let [some-inst (Instant/now)
          some-uuid (random-uuid)
          some-runtime-signal {:schema 1
                               :inst   some-inst
                               :ns     "marksto.telemere.axiom-test"
                               :coords [170 31]

                               :kind   :log
                               :level  :info
                               :id     ::id
                               :uid    some-uuid

                               :msg_   (delay "Forced message")
                               :data   {:attr-1 12345
                                        :attr-2 "abc"
                                        :attr-3 :kwd!
                                        :attr-4 [1 2]
                                        :attr-5 false}
                               :error  (ex-info "Error" {:error true})

                               :ctx    {:req-id 1234567890}

                               :host   {:name "localhost"
                                        :ip   "192.168.0.100"}
                               :thread {:name  "main"
                                        :id    1
                                        :group "main"}

                               :extra  :app-level-key}]
      (is (= {:_time  some-inst
              :ns     "marksto.telemere.axiom-test"
              :coords [170 31]

              :kind   :log
              :level  :info
              :id     ::id
              :uid    some-uuid

              :msg    "Forced message"
              :data   "{:attr-1 12345,\n :attr-2 \"abc\",\n :attr-3 :kwd!,\n :attr-4 [1 2],\n :attr-5 false}\n"
              :error  [{:type `ExceptionInfo
                        :msg  "Error"
                        :data {:error true}}]

              :ctx    {:req-id 1234567890}}
             (sut/default-prepare-fn some-runtime-signal))))))

(deftest handler:axiom-failed-test-signal-test
  (testing "failed test signal"
    (let [res (try
                (tel/with-handler
                  :axiom
                  (sut/handler:axiom {:conn-opts {:api-url   (fake-api-url:failure)
                                                  :api-token test-api-token
                                                  :dataset   test-dataset}})
                  (tel/log! :info "Unreachable message"))
                (catch Exception ex
                  ex))]
      (is (instance? Exception res)
          "An exception must be thrown")

      ;; NB: The handler is already stopped, so we can check the state.
      (let [test-state @*test-state]
        (is (= 1 (count (-> test-state :requests :failed)))
            "There must be exactly 1 failed requests")
        (is (empty? (-> test-state :requests :succeeded))
            "There must be no successful requests")

        (let [[first-batch & rest-batches] (:batches test-state)]
          (is (match? [test-signal-matcher] first-batch)
              "The first batch must consist of a test signal")
          (is (nil? rest-batches)
              "There must be no batches of any other signals"))))))

(deftest handler:axiom-successful-logging-test
  (testing "successful logging"
    (let [log-calls-num (inc (rand-int (- sut/max-batch-size 1000)))]
      (tel/with-handler
        :axiom
        (sut/handler:axiom {:conn-opts {:api-url   fake-api-url:success
                                        :api-token test-api-token
                                        :dataset   test-dataset}})
        (dotimes [n log-calls-num]
          (tel/log! {:level :info
                     :data  {:n n}
                     :msg   "Message"})))

      ;; NB: The handler is already stopped, so we can check the state.
      (let [test-state @*test-state]
        (is (empty? (-> test-state :requests :failed))
            "There must be no failed requests")
        (is (<= 2 (count (-> test-state :requests :succeeded)))
            "There must be at least 2 successful requests")

        (let [[first-batch & rest-batches] (:batches test-state)]
          (is (match? [test-signal-matcher] first-batch)
              "The first batch must consist of a test signal")
          (is (pos? (count rest-batches))
              "There must be at least 1 batch of log signals")

          (let [signals (mapcat identity rest-batches)]
            (is (= log-calls-num (count signals))
                "Num of signals sent must exactly match num of messages logged")
            (is (apply < (map #(-> % :data edn/read-string :n) signals))
                "Signals must be sent in the same order in which they were logged")))))))

(deftest handler:axiom-basic-signals-batching-test
  (testing "basic signals batching"
    (let [log-calls-num sut/max-batch-size
          total-batches 10
          batch-size (quot log-calls-num total-batches)]
      (tel/with-handler
        :axiom
        (sut/handler:axiom {:conn-opts  {:api-url   fake-api-url:success
                                         :api-token test-api-token
                                         :dataset   test-dataset}
                            :batch-size batch-size
                            :period-ms  1})
        (do (dotimes [n log-calls-num]
              (tel/log! {:level :info
                         :data  {:n n}
                         :msg   "Message"}))
            ;; NB: The final sleep must take long enough.
            (Thread/sleep (long (/ log-calls-num 2)))))

      ;; NB: The handler is already stopped, so we can check the state.
      (let [test-state @*test-state]
        (is (empty? (-> test-state :requests :failed))
            "There must be no failed requests")
        (is (<= 2 (count (-> test-state :requests :succeeded)))
            "There must be at least 2 successful requests")

        (let [[first-batch & rest-batches] (:batches test-state)]
          (is (match? [test-signal-matcher] first-batch)
              "The first batch must consist of a test signal")
          (is (<= total-batches (count rest-batches))
              (format "There must be at least %s batches of log signals"
                      total-batches))
          (is (every? #(<= (count %) batch-size) rest-batches)
              "Every batch of log signals must fit in size")

          (let [signals (mapcat identity rest-batches)]
            (is (= log-calls-num (count signals))
                "Num of signals sent must exactly match num of messages logged")
            (is (apply < (map #(-> % :data edn/read-string :n) signals))
                "Signals must be sent in the same order in which they were logged")))))))

(deftest handler:axiom-signals-flushing-test
  (testing "signals flushing (and how it affects signals batching)"
    (let [log-calls-num sut/max-batch-size
          total-batches 10
          batch-size (quot log-calls-num total-batches)]
      (tel/with-handler
        :axiom
        (sut/handler:axiom {:conn-opts  {:api-url   fake-api-url:success
                                         :api-token test-api-token
                                         :dataset   test-dataset}
                            :batch-size batch-size
                            :period-ms  1})
        (dotimes [n log-calls-num]
          (tel/log! {:level :info
                     :data  {:n n}
                     :msg   "Message"})))

      ;; NB: The handler is already stopped, so we can check the state.
      (let [test-state @*test-state]
        (is (empty? (-> test-state :requests :failed))
            "There must be no failed requests")
        (is (<= 2 (count (-> test-state :requests :succeeded)))
            "There must be at least 2 successful requests")

        (let [[first-batch & rest-batches] (:batches test-state)]
          (is (match? [test-signal-matcher] first-batch)
              "The first batch must consist of a test signal")
          (is (pos? (count rest-batches))
              "There must be at least 1 batch of log signals")
          (is (every? #(<= (count %) batch-size) (butlast rest-batches))
              "Every but the last batch of log signals must fit in size")
          (is (<= batch-size (count (last rest-batches)) sut/max-batch-size)
              "The last batch of log signals must meet the Axiom's limit")

          (let [signals (mapcat identity rest-batches)]
            (is (= log-calls-num (count signals))
                "Num of signals sent must exactly match num of messages logged")
            (is (apply < (map #(-> % :data edn/read-string :n) signals))
                "Signals must be sent in the same order in which they were logged")))))))



(comment
  ;; For a single test suite run
  (time (clojure.test/run-tests))

  ;; For hard-to-reproduce concurrency issues
  (loop []
    (let [res (time (clojure.test/run-tests))]
      (when (zero? (:fail res))
        (recur))))

  :end/comment)
