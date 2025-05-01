# telemere.axiom

[![Clojars Project](https://img.shields.io/clojars/v/com.github.marksto/telemere.axiom.svg)](https://clojars.org/com.github.marksto/telemere.axiom)

A set of things necessary for [Telemere](https://github.com/taoensso/telemere)-to-[Axiom](https://axiom.co) integration.

Defines and works through a customizable [signal handler](https://github.com/taoensso/telemere/wiki/4-Handlers).

## Documentation

Please, check out the `handler:axiom` function docstring.

## Usage

Here `<AXIOM_API_TOKEN>` and `<AXIOM_DATASET>` are the values that you've configured in Axiom. See the [Settings](https://axiom.co/docs/reference/settings) docs for more.

```clojure
(require '[marksto.telemere.axiom :as mta])
(require '[taoensso.telemere :as tt])

(def handler-opts
  {:conn-opts {:api-token <AXIOM_API_TOKEN>
               :dataset   <AXIOM_DATASET>}})

(def handler-fn (mta/handler:axiom handler-opts))

(tt/add-handler! :axiom handler-fn)
; or
(tt/add-handler! :axiom handler-fn <dispatch-opts>)
```

## License

Copyright &copy; 2025 [Mark Sto](https://github.com/marksto).  
Licensed under [EPL 1.0](LICENSE) (same as Clojure).
