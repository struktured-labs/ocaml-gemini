open! Common
open V1
module Currency_map = Map.Make (Currency.Enum_or_string)
module Symbol_map = Map.Make (Symbol.Enum_or_string)

module Update_source = struct
  module T = struct
    type t =
      [ `Market_data
      | `Trade
      | `External_trade
      ]
    [@@deriving sexp, equal, compare, enumerate]
  end

  include Json.Make (Json.Enum (T))
end

module type S = sig
  type t =
    { symbol : Symbol.Enum_or_string.t;
      pnl : float;
      position : float;
      spot : float;
      pnl_spot : float;
      notional : float;
      update_time : Timestamp.t;
      update_source : Update_source.t
    }
  [@@deriving sexp, compare, equal, fields, csv]

  val create :
    ?notional:float ->
    ?update_source:Update_source.t ->
    ?update_time:Timestamp.t ->
    symbol:Symbol.Enum_or_string.t ->
    unit ->
    t

  val on_trade :
    ?update_source:Update_source.t ->
    ?timestamp:Timestamp.t ->
    ?avg_trade_price:float ->
    t ->
    price:float ->
    side:Side.t ->
    qty:float ->
    t

  val from_mytrades :
    ?avg_trade_prices:float Symbol_map.t ->
    Mytrades.response ->
    t Symbol_map.t * t Pipe.Reader.t Symbol_map.t * t Pipe.Writer.t Symbol_map.t

  val update_spot : ?timestamp:Timestamp.t -> t -> float -> t

  val update_spots :
    ?timestamp:Timestamp.t ->
    t Symbol_map.t ->
    float Symbol_map.t ->
    t Symbol_map.t

  val command : string * Command.t
end

module T = struct
  type t =
    { symbol : Symbol.Enum_or_string.t;
      pnl : float;
      position : float;
      spot : float;
      pnl_spot : float;
      notional : float;
      update_time : Timestamp.t;
      update_source : Update_source.t
    }
  [@@deriving sexp, compare, equal, fields, csv]

  let create ?(notional = 0.0) ?(update_source = `Market_data) ?update_time
      ~(symbol : Symbol.Enum_or_string.t) () : t =
    { symbol;
      pnl = 0.;
      spot = Float.nan;
      notional;
      pnl_spot = 0.;
      position = 0.;
      update_source;
      update_time = Option.value_or_thunk update_time ~default:Timestamp.now
    }

  let rec on_trade ?(update_source = `Trade) ?timestamp
      ?(avg_trade_price : float option) t ~(price : float) ~(side : Side.t)
      ~(qty : float) : t =
    let timestamp = Option.value_or_thunk timestamp ~default:Timestamp.now in
    let position_sign =
      match side with
      | `Buy -> 1.0
      | `Sell -> -1.0
    in
    let position : float = t.position +. (qty *. position_sign) in
    let pnl_spot = price *. position in
    ( match Float.is_negative position with
    | true ->
      let qty = Float.abs position in
      let avg_trade_price = Option.value ~default:price avg_trade_price in
      let t =
        on_trade ~timestamp ~price:avg_trade_price ~side:(Side.opposite side)
          ~update_source:`External_trade ~qty t
      in
      on_trade ~update_source ~timestamp ~avg_trade_price ~price ~side ~qty t
    | false ->
      let notional_sign : float = position_sign *. -1.0 in
      let notional = (qty *. price) +. (notional_sign *. t.notional) in
      { t with
        spot = price;
        notional;
        pnl_spot;
        position;
        pnl = pnl_spot +. notional;
        update_time = timestamp;
        update_source
      } )
    |> fun t ->
    let sexp = sexp_of_t t in
    print_s sexp;
    t

  let update_spot ?timestamp t spot =
    let open Float in
    let update_time = Option.value_or_thunk timestamp ~default:Timestamp.now in
    let pnl_spot = t.position * spot in
    { t with
      spot;
      pnl_spot;
      pnl = t.notional + pnl_spot;
      update_time;
      update_source = `Market_data
    }

  let from_mytrades ?(avg_trade_prices : float Symbol_map.t option)
      (response : Mytrades.response) :
      t Symbol_map.t
      * t Pipe.Reader.t Symbol_map.t
      * t Pipe.Writer.t Symbol_map.t =
    let init : _ Symbol_map.t = Symbol_map.empty in
    let fold_f
        (symbol_map : (t * t Pipe.Reader.t * t Pipe.Writer.t) Symbol_map.t)
        (trade : Mytrades.trade) =
      let symbol : Symbol.Enum_or_string.t = trade.symbol in
      let update (t, reader, writer) =
        let price = Float.of_string trade.price in
        let side = trade.type_ in
        let qty = Float.of_string trade.amount in
        let avg_trade_price =
          Option.value ~default:Symbol_map.empty avg_trade_prices
          |> fun avg_trade_prices -> Map.find avg_trade_prices symbol
        in
        let timestamp = trade.timestamp in
        let t' = on_trade ?avg_trade_price t ~timestamp ~price ~side ~qty in
        ( t',
          reader,
          ( Pipe.write_without_pushback writer t';
            writer ) )
      in
      let f = function
        | None ->
          let t' = create ~symbol () in
          let reader, writer = Pipe.create () in
          update (t', reader, writer)
        | Some (t, reader, writer) -> update (t, reader, writer)
      in
      Core.Map.update symbol_map symbol ~f
    in
    let result =
      List.sort response ~compare:(fun x y ->
          Timestamp.compare x.timestampms y.timestampms )
      |> List.fold ~init ~f:fold_f
    in
    ( Map.map result ~f:(fun (last_t, _reader, _writer) -> last_t),
      Map.map result ~f:(fun (_last_t, reader, _writer) -> reader),
      Map.map result ~f:(fun (_last_t, _, writer) -> writer) )

  let update_spots ?timestamp (pnl : t Symbol_map.t)
      (prices : float Symbol_map.t) =
    Map.fold prices ~init:pnl ~f:(fun ~key:symbol ~data:price pnl ->
        Map.update pnl symbol ~f:(function
          | None -> create ~symbol ()
          | Some t -> update_spot ?timestamp t price ) )

  let update_from_book t book =
    update_spot t
      (Order_book.Book.market_price book ~side:`Bid ~volume:t.position
       |> function
       | Order_book.Price_level.{ price; volume } -> (
         match Float.equal volume t.position with
         | true -> price
         | false ->
           Log.Global.info
             "Volume estimate %f for price %f less than position %f" volume
             t.position price;
           price ) )

  let update_from_books (pnl : t Symbol_map.t) ~(books : Order_book.Books.t) :
      t Symbol_map.t =
    let f ~key:symbol ~data:t =
      Option.bind (Symbol.Enum_or_string.to_enum symbol) ~f:(fun symbol ->
          Order_book.Books.book books symbol
          |> Option.map ~f:(update_from_book t) )
    in
    Map.filter_mapi pnl ~f

  let update_from_book' ~(book : Order_book.Book.t) =
    update_from_books ~books:(Order_book.Books.(set_book empty) book)

  let pipes ?(how = `Sequential) cfg ~nonce ~(init : t Symbol_map.t) =
    let f ~key:symbol ~data:t =
      let symbol = Symbol.Enum_or_string.to_enum_exn symbol in
      Order_events.client ~nonce Order_book.Book.pipe cfg ~symbol ()
      >>| fun book_pipe ->
      Pipe.folding_map ~init:t book_pipe ~f:(fun t book ->
          match book with
          | `Ok book ->
            let t = update_from_book t book in
            (t, `Ok t)
          | #Market_data.Error.t as e -> (t, e) )
    in
    Deferred.Map.mapi ~how init ~f
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
    let symbol_of_string (s : string) =
      match Symbol.of_string_opt s with
      | Some symbol -> symbol
      | None -> (
        match Currency.of_string_opt s with
        | Some currency ->
          Symbol.of_currency_pair currency `Usd
          |> Option.value_exn ~here:[%here]
               ~message:(sprintf "Invalid currency %s" s)
        | None -> failwithf "Invalid symbol %s" s () )
    in

    Command.Param.(
      flag "--symbol"
        (optional (Command.Arg_type.create symbol_of_string))
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
                  Pipe.read_exactly ~num_values:10 book_pipe >>= function
                  | `Eof ->
                    failwithf
                      "Failed to read market data to get a spot price for \
                       symbol %s"
                      (Symbol.to_string symbol) ()
                  | `Exactly books
                  | `Fewer books -> (
                    Pipe.close_read book_pipe;
                    let t, readers, _ =
                      from_mytrades ?avg_trade_prices:None response
                    in
                    update_from_book t ~book:(Queue.last_exn books)
                    |> fun symbol_map ->
                    print_s (Symbol_map.sexp_of_t sexp_of_t symbol_map);

                    let csvable : Csv_writer.t =
                      Map.fold ~init:Csv_writer.empty
                        ~f:(fun ~key:_ ~data acc -> Csv_writer.add acc data)
                        symbol_map
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
    let t = T.create ~symbol:(Symbol.Enum_or_string.Enum `Ernusd) () in
    let t' = process_trades t trades in
    Log.Global.info_s (T.sexp_of_t t')
end
