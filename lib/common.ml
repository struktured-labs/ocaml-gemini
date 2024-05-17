(** Shared modules across various Gemini api endpoints *)

module Auth = Auth
module Result = Json.Result

module Int_number = struct
  type t = (int64[@encoding `number]) [@@deriving sexp, yojson, equal, compare]

  include (Csvfields.Csv.Atom (Int64) : Csvfields.Csv.Csvable with type t := t)
end

module Int_string = struct
  type t = (int64[@encoding `string]) [@@deriving sexp, yojson, equal, compare]

  include (Csvfields.Csv.Atom (Int64) : Csvfields.Csv.Csvable with type t := t)
end

module Decimal_number = struct
  type t = (float[@encoding `number]) [@@deriving sexp, yojson, equal, compare]
end

module Decimal_string = struct
  type t = string [@@deriving sexp, yojson, equal, compare]

  include (Csvfields.Csv.Atom (String) : Csvfields.Csv.Csvable with type t := t)

  let of_string t = t

  let to_string t = t
end

module Client_order_id = struct
  type t = string [@@deriving sexp, yojson, equal, compare]
end

(** Represents an order side. *)
module Side = struct
  module T = struct
    (** The type of an order side - [`Buy] or [`Sell]. *)
    type t =
      [ `Buy
      | `Sell
      ]
    [@@deriving sexp, enumerate, equal, compare]

    let to_string = function
      | `Buy -> "buy"
      | `Sell -> "sell"
  end

  include T

  include (Json.Make (T) : Json.S with type t := t)
end

(** Represents an exchange type. Only gemini is currently supported *)
module Exchange = struct
  module T = struct
    (** The exchange type - gemini only, currently. *)
    type t = [ `Gemini ] [@@deriving sexp, enumerate, equal, compare]

    let to_string `Gemini = "gemini"
  end

  include T

  include (Json.Make (T) : Json.S with type t := t)
end

(** Represents all styles of timestamps possibly returned by various gemini
    endpoints. *)
module Timestamp = struct
  module T0 = struct
    (** A timestamp is just a core time instance that was converted from some
        raw json date. *)
    type t = Time_float_unix.t [@@deriving sexp, equal, compare]

    let to_string t =
      Time_float_unix.to_span_since_epoch t
      |> Time_float_unix.Span.to_ms |> Float.to_string

    let to_yojson t = to_string t |> fun s -> `String s

    let of_yojson_with_span span_fn json =
      ( match json with
      | `String s -> `Ok (Float.of_string s)
      | `Int i -> `Ok (Float.of_int i)
      | `Int64 i -> `Ok (Float.of_int64 i)
      | #Yojson.Safe.t as json -> `Error json )
      |> function
      | `Error json ->
        Result.Error
          (sprintf "expected float as json but got %S"
             (Yojson.Safe.pretty_to_string json) )
      | `Ok f ->
        span_fn f |> Time_float_unix.of_span_since_epoch |> fun ok ->
        Result.Ok ok

    let of_yojson (ms : Yojson.Safe.t) =
      of_yojson_with_span Time_float_unix.Span.of_ms ms

    let of_string s =
      of_yojson (`String s) |> function
      | Result.Error e -> failwith e
      | Result.Ok x -> x
  end

  module T = struct
    include T0

    include (Csvfields.Csv.Atom (T0) : Csvfields.Csv.Csvable with type t := t)
  end

  module Ms = struct
    include T

    let of_yojson = of_yojson

    let to_yojson (ms : t) = to_yojson ms
  end

  module Sec = struct
    include T

    let of_yojson (sec : Yojson.Safe.t) =
      of_yojson_with_span Time_float_unix.Span.of_sec sec

    let to_yojson (sec : t) = to_yojson sec
  end

  include T
end

(** Represents currencies supported by Gemini. *)
module Currency = struct
  module T = struct
    (** An enumerated set of all currencies supported currently by Gemini. *)
    type t =
      [ `Eth
      | `Btc
      | `Doge
      | `Usd
      | `Zec
      | `Bch
      | `Ltc
      | `Luna
      | `Xtz
      | `Ust
      | `Link
      | `Aave
      | `Crv
      | `Inj
      | `Matic
      | `Ftm
      | `Cube
      | `Chz
      | `Dot
      | `Rare
      | `Qnt
      | `Sol
      | `Fet
      | `Imx
      | `Index
      | `Rndr
      | `Gala
      | `Rbn
      | `Mpl
      | `Metis
      | `Lqty
      | `Jam
      | `Fida
      | `Zbc
      | `Ali
      | `Eul
      | `Iotx
      | `Avax
      | `Pepe
      | `Ern
      | `Amp
      | `Ctx
      ]
    [@@deriving sexp, enumerate, equal, compare]
  end

  module Enum = Json.Enum (T)
  include Enum

  include (Json.Make (Enum) : Json.S with type t := t)
end

(** A symbol on the gemini exchange - Symbols are two currency names appended
    together which can be thought as "Buy the first one, Sell the second one".
    So [`Btcusd] implies you are buying btc and sell usd, effectively exchanging
    your currency from usd to btc in the process. *)
module Symbol = struct
  module T = struct
    (** The type of a symbol pair. See the [Symbol] module for details. *)
    type t =
      [ `Btcusd
      | `Ethusd
      | `Ethbtc
      | `Zecusd
      | `Zecbtc
      | `Zeceth
      | `Zecbch
      | `Zecltc
      | `Ltcusd
      | `Ltcbtc
      | `Ltceth
      | `Ltcbch
      | `Bchusd
      | `Bchbtc
      | `Bcheth
      | `Lunausd
      | `Xtzusd
      | `Linkusd
      | `Aaveusd
      | `Crvusd
      | `Injusd
      | `Maticusd
      | `Ftmusd
      | `Dogeusd
      | `Cubeusd
      | `Chzusd
      | `Dotusd
      | `Rareusd
      | `Qntusd
      | `Ernusd
      | `Ctxusd
      | `Ampusd
      | `Pepeusd
      | `Solusd
      | `Zbcusd
      | `Avaxusd
      ]
    [@@deriving sexp, enumerate, equal, compare]
  end

  include T
  module Enum = Json.Enum (T)
  include Enum

  let currency_pair_of_string s =
    (* TODO major hack, need to do something combinatorial instead, building an index over all enum pairs *)
    let buy =
      String.prefix s (String.length s - 3) |> Currency.Enum_or_string.of_string
    in
    let sell = String.suffix s 3 |> Currency.Enum_or_string.of_string in
    (buy, sell)

  let enum_or_string_to_currency_pair :
      [< t ] -> Currency.Enum_or_string.t * Currency.Enum_or_string.t = function
    | #t as c -> to_string c |> currency_pair_of_string

  let to_currency_pair : [< t ] -> (Currency.t * Currency.t) option = function
    | #t as c ->
      to_string c |> currency_pair_of_string |> fun (buy, sell) ->
      Option.both
        (Currency.Enum_or_string.to_enum buy)
        (Currency.Enum_or_string.to_enum sell)

  let of_currency_pair : Currency.t -> Currency.t -> t option =
   fun buy sell ->
    List.find all ~f:(fun symbol ->
        Option.value_map (to_currency_pair symbol) ~default:false
          ~f:(fun (buy', sell') ->
            Currency.(equal buy buy' && equal sell sell') ) )

  let to_currency : [< t ] -> Side.t -> Currency.Enum_or_string.t =
   fun t side ->
    enum_or_string_to_currency_pair t |> fun (buy, sell) ->
    match side with
    | `Buy -> buy
    | `Sell -> sell

  include (Json.Make (Enum) : Json.S with type t := t)

  let enum_or_string_to_currency enum_or_string ~side =
    match enum_or_string with
    | Enum_or_string.String s -> (
      let buy, sell = currency_pair_of_string s in
      match side with
      | `Buy -> buy
      | `Sell -> sell )
    | Enum_or_string.Enum e -> to_currency e side
end

(** Represents order types supported on Gemini. *)
module Order_type = struct
  module T = struct
    (** The type of order types- only [`Exchange_limit] is currently supported. *)
    type t =
      [ `Exchange_limit
      | `Stop_limit
      ]
    [@@deriving sexp, enumerate, equal, compare]

    let to_string = function
      | `Exchange_limit -> "exchange limit"
      | `Stop_limit -> "stop-limit"
  end

  include T

  include (Json.Make (T) : Json.S with type t := t)
end

(** The protocol version. *)
let v1 = "v1"
