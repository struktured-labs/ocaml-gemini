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

  val update_spots :
    t Currency_map.t -> float Currency_map.t -> t Currency_map.t

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
    List.sort response ~compare:(fun x y ->
        Timestamp.compare x.timestampms y.timestampms )
    |> List.fold ~init ~f:fold_f

  let update_spots (pnl : t Currency_map.t) (prices : float Currency_map.t) =
    Map.fold prices ~init:pnl ~f:(fun ~key:currency ~data:price pnl ->
        Map.update pnl currency ~f:(function
          | None -> create ~currency ()
          | Some t -> update_spot t price ) )

  let update_from_books (pnl : t Currency_map.t) ~(books : Order_book.Books.t) :
      t Currency_map.t =
    let f ~key:currency ~data:t =
      Currency.Enum_or_string.to_enum currency
      |> Option.bind ~f:(fun currency ->
             let symbol = Symbol.of_currency_pair currency `Usd in
             Option.bind symbol ~f:(fun symbol ->
                 Order_book.Books.book books symbol )
             |> Option.map ~f:(fun book ->
                    update_spot t
                      (Order_book.Book.market_price book ~side:`Bid
                         ~volume:t.position
                       |> function
                       | Order_book.Price_level.{ price; volume } -> (
                         match Float.equal volume t.position with
                         | true -> price
                         | false ->
                           Log.Global.info
                             "Volume estimate %f for price %f less than \
                              position %f"
                             volume t.position price;
                           price ) ) ) )
    in
    Map.filter_mapi pnl ~f

  let update_from_book ~(book : Order_book.Book.t) =
    update_from_books ~books:(Order_book.Books.(set_book empty) book)
end

module TT : S = struct
  module Csv_writer = Csv_support.Writer (T)
  include T

  let timestamp_param =
    Command.Param.(
      flag "-ts"
        (optional (Command.Arg_type.create Time_float_unix.of_string))
        ~doc:
          "TIMESTAMP Return trades before or equal to the given unix timestamp." )

  let limit_trades_param =
    Command.Param.(
      flag "-lt" (optional int) ~doc:"INT Limit the number of trades." )

  let symbol_param =
    Command.Param.(
      flag "--symbol"
        (optional (Command.Arg_type.create Symbol.of_string))
        ~doc:"STRING Symbol to compute PNL over. Defaults to all." )

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
            let symbols, is_one_symbol =
              Option.value_map symbol
                ~f:(fun x -> ([ x ], true))
                ~default:(Symbol.all, false)
            in
            let config = Cfg.or_default config in
            Deferred.List.iter ~how:`Sequential symbols ~f:(fun symbol ->
                let request : Mytrades.request =
                  Mytrades.{ timestamp; limit_trades; symbol }
                in
                Nonce.File.(pipe ~init:default_filename) () >>= fun nonce ->
                let nonce, writer_nonce =
                  Inf_pipe.fork ~pushback_uses:`Fast_consumer_only nonce
                in
                Mytrades.post config nonce request >>= function
                | `Ok response -> (
                  Order_book.Book.pipe_exn config ~symbol ()
                  >>= fun book_pipe ->
                  Pipe.read_exactly ~num_values:100 book_pipe >>= function
                  | `Eof -> failwith "eof"
                  | `Exactly books
                  | `Fewer books -> (
                    Pipe.close_read book_pipe;
                    from_mytrades response
                    |> update_from_book ~book:(Queue.last_exn books)
                    |> fun currency_map ->
                    print_s (Currency_map.sexp_of_t sexp_of_t currency_map);

                    let csvable : Csv_writer.t =
                      Map.fold ~init:Csv_writer.empty
                        ~f:(fun ~key:_ ~data acc -> Csv_writer.add acc data)
                        currency_map
                    in
                    Inf_pipe.read writer_nonce >>= fun request_id ->
                    let name =
                      sprintf "pnl.%s.%d"
                        ( Symbol.to_currency symbol `Buy
                        |> Currency.Enum_or_string.to_string |> String.lowercase
                        )
                        request_id
                    in
                    let _ : int = Csv_writer.write ?dir:None ~name csvable in
                    Log.Global.flushed () >>= fun () ->
                    match is_one_symbol with
                    | true -> Deferred.unit
                    | false -> Clock.after (Time_float.Span.of_int_sec 1) ) )
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
