open! Common
open V1

module Currency_map = struct
  module T = Map.Make (Currency.Enum_or_string)
  include T
end

module type S = sig
  type t =
    { currency : Currency.Enum_or_string.t;
      pnl : float;
      position : float;
      spot : float;
      pnl_spot : float;
      notional : float
    }
  [@@deriving sexp, compare, equal, fields, csv]

  val create :
    ?notional:float -> currency:Currency.Enum_or_string.t -> unit -> t

  val on_trade :
    ?notional:float -> t -> price:float -> side:Side.t -> qty:float -> t

  val from_mytrades : Mytrades.response -> t Currency_map.t

  val update_spot : t -> float -> t

  val command : string * Command.t
end

module T = struct
  type t =
    { currency : Currency.Enum_or_string.t;
      pnl : float;
      position : float;
      spot : float;
      pnl_spot : float;
      notional : float
    }
  [@@deriving sexp, compare, equal, fields, csv]

  let create ?(notional = 0.0) ~(currency : Currency.Enum_or_string.t) () : t =
    { currency;
      pnl = 0.;
      spot = Float.nan;
      notional;
      pnl_spot = 0.;
      position = 0.
    }

  let on_trade ?(notional : float option) t ~(price : float) ~(side : Side.t)
      ~(qty : float) : t =
    let position_sign =
      match side with
      | `Buy -> 1.0
      | `Sell -> -1.0
    in
    let position : float = t.position +. (qty *. position_sign) in
    let notional_sign : float = position_sign *. -1.0 in
    let notional : float =
      Option.value_or_thunk notional ~default:(fun () -> qty *. price)
      *. notional_sign
      +. t.notional
    in
    let pnl_spot = price *. position in
    let t =
      { t with
        spot = price;
        notional;
        pnl_spot;
        position;
        pnl = pnl_spot +. notional
      }
    in
    let sexp = sexp_of_t t in
    print_s sexp;
    t

  let update_spot t spot =
    let open Float in
    let pnl_spot = t.position * spot in
    { t with spot; pnl_spot; pnl = t.notional + pnl_spot }

  let from_mytrades (response : Mytrades.response) =
    let init : t Currency_map.t = Currency_map.empty in
    let fold_f (currency_map : t Currency_map.t) (trade : Mytrades.trade) =
      let symbol : Symbol.Enum_or_string.t = trade.symbol in
      let currency = Symbol.enum_or_string_to_currency symbol ~side:`Buy in
      let sell_currency =
        Symbol.enum_or_string_to_currency symbol ~side:`Sell
      in
      let update (t : t) =
        let notional = None in
        let price = Float.of_string trade.price in
        let side = trade.type_ in
        let qty = Float.of_string trade.amount in
        on_trade t ?notional ~price ~side ~qty
      in
      match Currency.Enum_or_string.promote sell_currency with
      | Some `Usd ->
        let f = function
          | None -> update (create ~currency ())
          | Some x -> update x
        in
        Core.Map.update currency_map currency ~f
      | Some (_ : Currency.t)
      | None ->
        (*failwiths ~here:[%here] "Non usd trades unsupported: " sell_currency
          Currency.Enum_or_string.sexp_of_t*)
        Log.Global.error_s (sell_currency |> Currency.Enum_or_string.sexp_of_t);
        currency_map
    in

    List.fold ~init ~f:fold_f response
end

module TT : S = struct
  module Csv_writer = Csv_support.Writer (T)
  include T

  let timestamp_param =
    Command.Param.(
      flag "-ts"
        (optional (Command.Arg_type.create Time_float_unix.of_string))
        ~doc:"Timestamp" )

  let limit_trades_param =
    Command.Param.(flag "-lt" (optional int) ~doc:"Limit trades")

  let symbol_param =
    Command.Param.(
      flag "-sy"
        (optional (Command.Arg_type.create Symbol.of_string))
        ~doc:"symbol" )

  let command : string * Command.t =
    let operation_name = "pnl" in
    let open Command.Let_syntax in
    ( operation_name,
      Command.async
        ~summary:(Path.to_summary ~has_subnames:false [ operation_name ])
        [%map_open
          let timestamp = timestamp_param
          and config = Cfg.param
          and limit_trades = limit_trades_param
          and symbol = symbol_param in
          fun () ->
            Deferred.List.iter ~how:`Sequential
              (Option.value_map symbol ~f:(fun x -> [ x ]) ~default:Symbol.all)
              ~f:(fun symbol ->
                let request : Mytrades.request =
                  Mytrades.{ timestamp; limit_trades; symbol }
                in
                let config = Cfg.or_default config in
                Nonce.File.(pipe ~init:default_filename) () >>= fun nonce ->
                let nonce, writer_nonce =
                  Inf_pipe.fork ~pushback_uses:`Fast_consumer_only nonce
                in
                Mytrades.post config nonce request >>= function
                | `Ok response ->
                  from_mytrades response |> fun currency_map ->
                  print_s (Currency_map.sexp_of_t sexp_of_t currency_map);

                  let csvable : Csv_writer.t =
                    Map.fold ~init:Csv_writer.empty
                      ~f:(fun ~key:_ ~data acc -> Csv_writer.add acc data)
                      currency_map
                  in
                  Inf_pipe.read writer_nonce >>= fun request_id ->
                  let name = sprintf "pnl.%d" request_id in
                  let _ : int = Csv_writer.write ?dir:None ~name csvable in
                  Log.Global.flushed ()
                | #Rest.Error.post as post_error ->
                  failwiths ~here:[%here]
                    (sprintf "post for operation %S failed"
                       (Path.to_string [ operation_name ]) )
                    post_error Rest.Error.sexp_of_post )] )
end

include TT

module Test = struct
  let process_trades t trades =
    List.fold_left ~init:t
      ~f:(fun t (price, side, qty) -> T.on_trade t ~price ~side ~qty)
      trades

  let test1 () =
    let trades = [ (1.0, `Buy, 10.0) ] in
    let t = T.create ~currency:(Currency.Enum_or_string.Enum `Ern) () in
    let t' = process_trades t trades in
    Log.Global.info_s (T.sexp_of_t t')
end
