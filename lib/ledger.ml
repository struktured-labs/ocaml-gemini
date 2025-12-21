open! Common
module Currency_map = Map.Make (Currency.Enum_or_string)

module Mytrades = V1.Mytrades
module Order_events = V1.Order_events


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

module T = struct
  type t =
    { symbol : Symbol.Enum_or_string.t;
      pnl : float [@default 0.0];
      position : float [@default 0.0];
      spot : float [@default 0.0];
      pnl_spot : float [@default 0.0];
      notional : float [@default 0.0];
      avg_buy_price: float [@default 0.0];
      avg_sell_price: float [@default 0.0];
      avg_price: float [@default 0.0];
      update_time : Timestamp.t;
      update_source : Update_source.t [@default `Market_data];
      total_buy_qty: float [@default 0.0];
      total_sell_qty: float [@default 0.0];
      price: Price.Option.t [@default None];
      side: Side.Option.t [@default None];
      qty: Decimal_number.Option.t [@default None];
      package_price: Price.Option.t [@default None];
      buy_notional: float [@default 0.0];
      sell_notional: float [@default 0.0];
      total_original: float [@default 0.0];
      total_executed: float [@default 0.0];
      total_remaining: float [@default 0.0];
      cost_basis: float [@default 0.0];
      running_price: float [@default 0.0];
      running_qty: float [@default 0.0]
  }
  [@@deriving sexp, compare, equal, fields, csv, make]

  let create ?update_time =
    let update_time = Option.value_or_thunk update_time ~default:Timestamp.now in
    make_t ~update_time
    
  let rec on_trade ?(update_source = `Trade) ?timestamp
      ?(avg_trade_price : float option) ?(fee_usd : float = 0.) t
      ~(price : float) ~(side : Side.t) ~(qty : float) : t =
    Log.Global.info "on_trade: price=%f side=%s qty=%f fee_usd=%f" price
      (Side.to_string side) qty fee_usd;
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
      let package_price = qty *. price in
      (* Fees reduce cash: apply with negative sign to notional regardless of side. *)
      let signed_notional = (notional_sign *. package_price) -. fee_usd in
      let notional = signed_notional +. t.notional in
      let total_buy_qty, total_sell_qty =
       match side with 
       | `Buy -> (t.total_buy_qty +. qty), t.total_sell_qty
       | `Sell -> t.total_buy_qty, (t.total_sell_qty+.qty) in
       let avg_buy_price, avg_sell_price = 
        match side with
        | `Buy -> (t.avg_buy_price *. t.total_buy_qty +. price *. qty) /. total_buy_qty, t.avg_sell_price 
        | `Sell -> t.avg_buy_price, (t.avg_sell_price *. t.total_sell_qty +. price *. qty) /. total_sell_qty in
      let avg_price = (avg_buy_price *. total_buy_qty +. avg_sell_price *. total_sell_qty)  /. (total_buy_qty +. total_sell_qty) in
      let buy_notional, sell_notional = 
        match side with
        | `Buy -> t.buy_notional +. package_price, t.sell_notional
        | `Sell -> t.buy_notional, t.sell_notional +. package_price in
      let cost_basis =
        match side with
        | `Buy -> t.cost_basis +. package_price +. fee_usd
        | `Sell ->
          (* Fee on sell reduces proceeds; keep cost basis adjustment as before. *)
          t.cost_basis -. t.cost_basis *. qty /. t.running_qty in
      let running_qty = 
        match side with
        | `Buy -> t.running_qty +. qty
        | `Sell -> t.running_qty -. qty in
      let running_price =
        match running_qty with
        | 0. -> 0.
        | _ -> cost_basis /. running_qty in
      Log.Global.info "package_price=%f t.notional=%f notional_sign=%f notional=%f signed_t_notitional=%f" package_price t.notional notional_sign notional signed_notional;
      { t with
        spot = price;
        notional;
        pnl_spot;
        position;
        pnl = pnl_spot +. notional;
        update_time = timestamp;
        update_source;
        total_buy_qty;
        price=Some price;
        package_price=Some package_price;
        total_sell_qty;
        avg_buy_price;
        avg_sell_price;
        avg_price;
        qty=Some qty;
        side=Some side;
        buy_notional;
        sell_notional;
        running_price;
        running_qty;
        cost_basis
      } )
    |> fun t ->
    let sexp = sexp_of_t t in
    print_s sexp;
    t

  let update_spot ?timestamp t spot =
    let open Float in
    let update_time = Option.value_or_thunk timestamp ~default:Timestamp.now in
    let pnl_spot = t.position *. spot in
    let is_nan x = match Float.classify x with | Float.Class.Nan -> true | _ -> false in
    let bad = (t.position = 0.0) || is_nan spot || is_nan pnl_spot in
    let pnl_spot, pnl =
      match bad with
      | true ->
        Log.Global.info
          "[Ledger] Zero/NaN detected in update_spot: position=%f spot=%f pnl_spot=%f. Setting pnl_spot=0 and pnl=notional only."
          t.position spot pnl_spot;
        (0.0, t.notional)
      | false -> (pnl_spot, t.notional +. pnl_spot)
    in
    { t with
      spot;
      pnl_spot;
      pnl;
      update_time;
      update_source = `Market_data;
      side=None;
      price=None;
      qty=None;
      package_price=None;
    }

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

  type event =
    [ `Order_event of Order_events.Order_event.t
    | `Order_book of Order_book.Book.t
    ]
  [@@deriving sexp] 
 
  let on_summary t (summary:Order_tracker.summary) =
    let Order_tracker.{total_original; total_executed; total_remaining} = summary in
    { t with
      total_original;
      total_executed;
      total_remaining;
    }
  let pipe ~init ?num_values ?behavior (order_book : Order_book.Book.t Pipe.Reader.t)
      (order_events : Order_events.response Pipe.Reader.t) =
    
    let order_book =
      Pipe.map order_book ~f:(fun t -> (`Order_book t :> event))
    in
    let order_events =
      Pipe.concat_map_list order_events ~f:(function
        | `Order_event e -> [ e ]
        | `Order_events ee -> ee
        | _ -> [] )
      |> Pipe.filter_map ~f:(fun (o : Order_events.Order_event.t) ->
             match Symbol.Enum_or_string.equal o.symbol init.symbol with
             | true -> Some (`Order_event o :> event)
             | false -> None )
    in
    return @@ Pipe_ext.combine ?num_values ?behavior order_book order_events
    >>| fun pipe ->
      let init = (init, Order_tracker.empty) in
      Pipe.folding_map pipe ~init ~f:(fun (t, order_tracker) e ->
        match e with
        | `Order_book book -> let t = update_from_book t book in ((t, order_tracker), t)
        | `Order_event event -> (
          let order_tracker = Order_tracker.on_order_event order_tracker event in
          let fee_usd =
            match event.fill with
            | Some Order_events.Fill.{ fee; fee_currency; price; _ } -> (
                try
                  let fee_amount = Float.of_string fee in
                  let base_currency =
                    Symbol.enum_or_string_to_currency event.symbol ~side:`Buy
                  in
                  let quote_currency =
                    Symbol.enum_or_string_to_currency event.symbol ~side:`Sell
                  in
                  let fee_usd =
                    match Currency.Enum_or_string.equal fee_currency quote_currency with
                    | true -> fee_amount
                    | false ->
                      (match Currency.Enum_or_string.equal fee_currency base_currency with
                      | true -> fee_amount *. Float.of_string price
                      | false -> 0.)
                  in
                  Some fee_usd
                with _ -> None )
            | None -> None
          in
          (match event.fill with
           | Some (Order_events.Fill.{amount; price; _}) ->
               let summary = Order_tracker.summary order_tracker in
               let timestamp = event.timestampms in
               let qty = Float.of_string amount in
               let price = Float.of_string price in
               let side = event.side in
               let fee_usd = Option.value fee_usd ~default:0. in
               let t =
                 on_trade t ~timestamp ~side ~price ~qty ~fee_usd
                 |> fun t -> on_summary t summary
               in
               ((t, order_tracker), t)
           | None ->
               (let summary = Order_tracker.summary order_tracker in
                on_summary t summary |> fun t ->
                ((t, order_tracker), t) )))
      )

      

      
  let _pipe ?notional ?update_time ?update_source ~symbol = 
    pipe ~init:(create ~symbol ?notional ?update_time ?update_source ())

  let from_mytrades ?(init : t Symbol.Enum_or_string.Map.t option)
      ?(avg_trade_prices : float Symbol.Enum_or_string.Map.t option)
      (response : Mytrades.trade list) :
      t Symbol.Enum_or_string.Map.t * t Pipe.Reader.t Symbol.Enum_or_string.Map.t =
    let init : _ Symbol.Enum_or_string.Map.t = Option.value init ~default:Symbol.Enum_or_string.Map.empty in
    let fold_f
        (symbol_map : (t * t Pipe.Reader.t * t Pipe.Writer.t) Symbol.Enum_or_string.Map.t)
        (trade : Mytrades.trade) =
      let symbol : Symbol.Enum_or_string.t = trade.symbol in
      let update (t, reader, writer) =
        let price = Float.of_string trade.price in
        let side = trade.type_ in
        let qty = Float.of_string trade.amount in
        let avg_trade_price =
          Option.value ~default:Symbol.Enum_or_string.Map.empty avg_trade_prices
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
          let entry = create ~symbol () in
          let reader, writer = Pipe.create () in
          update (entry, reader, writer)
        | Some (t, reader, writer) -> update (t, reader, writer)
      in
      Core.Map.update symbol_map symbol ~f
    in
    let init =
      Map.map init ~f:(fun t ->
          let reader, writer = Pipe.create () in
          (t, reader, writer) )
    in
    let result =
      List.sort response ~compare:(fun x y ->
          Timestamp.compare x.timestampms y.timestampms )
      |> List.fold ~init ~f:fold_f
    in
    ( Map.map result ~f:(fun (last_t, _reader, _writer) -> last_t),
      Map.map result ~f:(fun (_last_t, reader, _writer) -> reader) )

  let from_mytrades_pipe ?symbols ?init ?timestamp ?(how = `Sequential) ?avg_trade_prices ?nonce
      (module Cfg : Cfg.S) order_events =
            (match nonce with
            | Some nonce -> return nonce
            | None -> Nonce.File.default ()) >>= fun nonce ->
    let symbols = Option.value symbols ~default:(Symbol.all |> List.map ~f:Symbol.Enum_or_string.of_enum) in
    let order_events =
      List.folding_map ~init:order_events symbols ~f:(fun oe symbol ->
          let oe, oe' = Pipe.fork ~pushback_uses:`Fast_consumer_only oe in
          (oe', (symbol, oe)) )
      |> Symbol.Enum_or_string.Map.of_alist_exn
    in
    Deferred.Map.mapi ~how order_events ~f:(fun ~key:enum_or_str_symbol ~data:order_events ->
        let symbol = Symbol.Enum_or_string.to_enum_exn enum_or_str_symbol in
        Order_book.Book.pipe_exn (module Cfg) ~symbol () >>= fun order_book ->
         Mytrades.post (module Cfg) nonce Mytrades.{symbol;timestamp;limit_trades=None} >>= fun trades ->
         let init, trades_by_symbol = from_mytrades ?init ?avg_trade_prices 
          (Poly_ok.ok_exn ~message:"Error processing my own trades" ~here:[%here] trades) in
         let init = Map.find init enum_or_str_symbol |> Option.value ~default:(create ~symbol:enum_or_str_symbol ()) in
         let trade_pipe = Map.find trades_by_symbol enum_or_str_symbol |> Option.value_or_thunk ~default:Pipe.empty in
         pipe ~init order_book order_events >>| Pipe_ext.combine trade_pipe)
  
  let from_balances ?(notional_currency = `Usd) (response : V1.Balances.balance list) :
      t Symbol.Enum_or_string.Map.t =
    List.fold response ~init:Symbol.Enum_or_string.Map.empty ~f:(fun acc balance ->
      let currency = balance.currency in
      let position = Float.of_string balance.amount in
      (* Only create entries for non-zero positions *)
      match Float.(position > 0.) with
      | false -> acc
      | true ->
        (* For each currency, try to map it to a trading pair with notional_currency *)
        let open Option.Let_syntax in
        let symbol_opt = 
          let%bind currency_enum = Currency.Enum_or_string.to_enum currency in
          let%map symbol = Symbol.of_currency_pair currency_enum notional_currency in
          Symbol.Enum_or_string.of_enum symbol
        in
        match symbol_opt with
        | None -> acc
        | Some symbol ->
          let entry = create ~symbol ~position () in
          Map.set acc ~key:symbol ~data:entry)

end

module Entry (*: ENTRY *) = struct
  module Csv_writer = Csv_support.Writer (T)
  include T
end
(**
module type S = sig
  type t = (Entry.t Symbol.Enum_or_string.Map.t[@deriving sexp, equal, compare])

  val from_mytrades :
    ?avg_trade_prices:float Symbol.Enum_or_string.Map.t ->
    Mytrades.response ->
    t * Entry.t Pipe.Reader.t

  val update_spots : ?timestamp:Timestamp.t -> t -> float Symbol.Enum_or_string.Map.t -> t

  val command : string * Command.t
end
*)
module Ledger (*: S *) = struct
  type t = Entry.t Symbol.Enum_or_string.Map.t [@@deriving sexp, compare, equal]

  (*
  type event =
    { ledger : t;
      symbol : Symbol.Enum_or_string.t;
      entry : Entry.t
    }
  [@@deriving sexp, compare, equal]
*)
  let update_from_books (pnl : t) ~(books : Order_book.Books.t) : t =
    let f ~key:symbol ~data:t =
      Option.bind (Symbol.Enum_or_string.to_enum symbol) ~f:(fun symbol ->
          Order_book.Books.book books symbol
          |> Option.map ~f:(Entry.update_from_book t) )
    in
    Map.filter_mapi pnl ~f

  let update_from_book' t ~(book : Order_book.Book.t) =
    update_from_books t ~books:(Order_book.Books.(set_book empty) book)

  let on_trade' ?update_source ?timestamp ?(avg_trade_price : float option)
      ?(fee_usd : float = 0.) (t : t) ~symbol ~(price : float)
      ~(side : Side.t) ~(qty : float) : t =
    Map.update t symbol ~f:(fun t ->
        let t = Option.value_or_thunk t ~default:(Entry.create ~symbol) in
        Entry.on_trade ?update_source ?timestamp ?avg_trade_price ~fee_usd ~qty
          ~price ~side t )

  let on_order_events (t : t) (events : Order_events.Order_event.t list) =
    let events =
      List.filter events ~f:(fun event ->
          Order_events.Order_event_type.equal event.type_ `Fill )
    in
    List.fold events ~init:t ~f:(fun t event ->
        let price =
          Option.value_exn event.avg_execution_price |> Float.of_string
        in
        let qty = Option.value_exn event.executed_amount |> Float.of_string in
        let symbol = event.symbol in
        let side = event.side in
        on_trade' t ~symbol ~timestamp:event.timestampms ~side ~price ~qty )

  let on_order_event_response t response =
    on_order_events t (Order_events.order_events_of_response response)

  let update_spots ?timestamp (pnl : t) (prices : float Symbol.Enum_or_string.Map.t) =
    Map.fold prices ~init:pnl ~f:(fun ~key:symbol ~data:price pnl ->
        Map.update pnl symbol ~f:(function
          | None -> Entry.create ~symbol ()
          | Some t -> Entry.update_spot ?timestamp t price ) )

  let with_csv_writer ?(how=`Parallel) ?dir ?timestamp ?avg_trade_prices config symbols =
    Nonce.File.default () >>= fun nonce ->
    Order_events.client config ~nonce () >>= fun order_events -> 
      (Pipe.map order_events ~f:(Poly_ok.ok_exn ?sexp_of_error:None ~here:[%here] ~message:"Order events error" )
      |> return) >>= fun order_events ->
    T.from_mytrades_pipe ~how ?timestamp ?avg_trade_prices ~symbols ~nonce config order_events >>=
    fun symbol_to_reader ->
      let result = Deferred.Map.mapi ~how symbol_to_reader ~f:
        (fun ~key:symbol ~data:reader ->
          let name = sprintf "pnl.%s" (Symbol.Enum_or_string.to_string symbol) in
          let csv_read entry = 
            let csv = Entry.Csv_writer.(add empty entry) in
            let num_written = Entry.Csv_writer.write ?dir ~name csv in
            Log.Global.info "wrote %d record(s) to %s" num_written name;
            entry in
          Pipe.map reader ~f:csv_read |> return
        ) in result


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
(**
Command does what?

- Computes pnl for given set of symbols (or all)
- Does so by getting latest spot prices (from order book)
- Connects to order event pipe, trade pipe, trade list
- Produces time order pnl entries up to present moment (or let it run and produce a stream)
- TODO: capture trades > 500 with multiple requests
- Allow synthetic trades or estimates of avg if needed by calculation.
  (for wallet xfers with unknown purchase prices)

*)
  let command : string * Command.t =
    let operation_name = "ledger" in
    let open Command.Let_syntax in
    ( operation_name,
      Command.async
        ~summary:(Path.to_summary ~has_subnames:false [ operation_name ])
        [%map_open
          let _timestamp = timestamp_param
          and config = Cfg.param
          and _limit_trades = limit_trades_param
          and symbol = symbol_param in
          fun () ->
            let symbols = Option.value_map ~default:Symbol.all symbol ~f:List.singleton in
            let symbols = List.map ~f:Symbol.Enum_or_string.of_enum symbols in
            let config = Cfg.or_default config in
          with_csv_writer config symbols >>= fun symbol_reader -> 
            Deferred.Map.iter ~how:`Sequential symbol_reader ~f:(fun reader -> 
              let f = function 
              | T.{update_source=`Market_data;_} -> true 
              | _ -> false
              in
              Pipe.filter reader ~f |>
              Pipe.read_exactly ?consumer:None ~num_values:1 >>= fun _ -> Deferred.unit)])


  let pipe ?num_values ?behavior ?(how=`Sequential) ~(init:Entry.t Symbol.Map.t) (order_books:Order_book.Book.t Pipe.Reader.t Symbol.Map.t) order_events =
    Deferred.Map.map ~how init ~f:(fun entry ->  
      let order_book = entry.symbol |> Symbol.Enum_or_string.to_enum_exn (* TODO: Rethink this *) |> Map.find order_books in
      let order_book = Option.value_exn order_book in
      Entry.pipe ~init:entry ?num_values ?behavior order_book order_events)
  end

include Ledger
