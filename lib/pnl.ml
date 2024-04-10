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

  val from_trade_vol : Tradevolume.response -> t Currency_map.t

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
    printf "on_trade notional=%f price=%f qty=%f side=%s\n"
      (Option.value notional ~default:0.0)
      price qty (Side.to_string side);
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
      match Currency.Enum_or_string.promote sell_currency with
      | Some `Usd ->
        let f = function
          | None -> f (create ~currency ())
          | Some x -> f x
        in
        Core.Map.update currency_map currency ~f
      | Some (_ : Currency.t)
      | None ->
        failwiths ~here:[%here] "Non usd trades unsupported: " sell_currency
          Currency.Enum_or_string.sexp_of_t
    in
    List.fold ~init ~f response
end

module TT : S = struct
  module Csv_writer = Csv_support.Writer (T)
  include T

  let command : string * Command.t =
    let operation_name = "pnl" in
    let open Command.Let_syntax in
    ( operation_name,
      Command.async
        ~summary:(Path.to_summary ~has_subnames:false [ operation_name ])
        [%map_open
          let config = Cfg.param in
          fun () ->
            let request = () in
            let config = Cfg.or_default config in
            Nonce.File.(pipe ~init:default_filename) () >>= fun nonce ->
            Tradevolume.post config nonce request >>= function
            | `Ok response ->
              from_trade_vol response |> fun currency_map ->
              print_s (Currency_map.sexp_of_t sexp_of_t currency_map);

              let csvable : Csv_writer.t =
                Map.fold ~init:Csv_writer.empty
                  ~f:(fun ~key:_ ~data acc -> Csv_writer.add acc data)
                  currency_map
              in
              let _ : int = Csv_writer.write ?dir:None ~name:"pnl" csvable in
              Log.Global.flushed ()
            | #Rest.Error.post as post_error ->
              failwiths ~here:[%here]
                (sprintf "post for operation %S failed"
                   (Path.to_string [ operation_name ]) )
                post_error Rest.Error.sexp_of_post] )
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
