open! Common
open V1
module Currency_map = Map.Make (Currency.Enum_or_string)

module type S = sig
  type t =
    { currency : Currency.Enum_or_string.t;
      pnl : float;
      position : float;
      spot : float;
      pnl_spot : float;
      notional : float
    }
  [@@deriving sexp, compare, equal, fields]

  val create :
    ?notional:float -> currency:Currency.Enum_or_string.t -> unit -> t

  val on_trade :
    ?notional:float -> t -> price:float -> side:Side.t -> qty:float -> t

  val from_trade_vol : Tradevolume.response -> t Currency_map.t

  val update_spot : t -> float -> t
end

module T : S = struct
  type t =
    { currency : Currency.Enum_or_string.t;
      pnl : float;
      position : float;
      spot : float;
      pnl_spot : float;
      notional : float
    }
  [@@deriving sexp, compare, equal, fields]

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
    let open Float in
    let sign =
      match side with
      | `Buy -> -1.0
      | `Sell -> 1.0
    in
    let qty = sign * qty in
    let position = t.position + qty in
    let notional =
      Option.value_or_thunk notional ~default:(fun () -> qty * price)
    in
    let pnl_spot = price * position in
    { t with
      spot = price;
      notional;
      pnl_spot;
      position;
      pnl = pnl_spot + notional
    }

  let update_spot t spot =
    let open Float in
    let pnl_spot = t.position * spot in
    { t with spot; pnl_spot; pnl = t.notional + pnl_spot }

  let from_trade_vol (response : Tradevolume.response) =
    let init : t Currency_map.t = Currency_map.empty in
    let f (currency_map : t Currency_map.t) (volume : Tradevolume.volume) =
      let symbol : Symbol.Enum_or_string.t = volume.symbol in
      let process_field (t : t) ~side ~base ~notional : t =
        let qty = Field.get base volume in
        match Float.equal qty 0. with
        | true -> t
        | false ->
          let notional = Field.get notional volume in
          let price = notional /. qty in
          on_trade ~notional t ~price ~side ~qty
      in
      let fields_to_process =
        Tradevolume.Fields_of_volume.
          [ (`Buy, buy_taker_base, buy_taker_notional);
            (`Buy, buy_maker_base, buy_maker_notional);
            (`Sell, sell_maker_base, sell_maker_notional);
            (`Sell, sell_taker_base, sell_taker_notional)
          ]
      in
      let f (t : t) (side, base, notional) : t =
        process_field t ~side ~base ~notional
      in
      let f t = List.fold ~init:t ~f fields_to_process in
      let currency = Symbol.enum_or_string_to_currency symbol ~side:`Buy in
      let sell_currency =
        Symbol.enum_or_string_to_currency symbol ~side:`Sell
      in
      match sell_currency with
      | Currency.Enum_or_string.Enum `Usd ->
        let f = function
          | None -> f (create ~currency ())
          | Some x -> f x
        in
        Core.Map.update currency_map currency ~f
      | _ -> failwith "Non usd trades unsupported"
    in
    List.fold ~init ~f response
end

include T

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
