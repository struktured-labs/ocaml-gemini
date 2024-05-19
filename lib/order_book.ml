open! Common
open V1
module Symbol_map = Map.Make (Symbol)

(*
module Inverted_float : Comparator. = struct
  type t = float [@@deriving equal, sexp]

  let compare t t' = Float.compare t t' * -1

  let comparator = compare
end
*)

module Price = struct
  include Float
end

module Bid_price = struct
  include Float

  include Comparator.Make (struct
    type t = float [@@deriving sexp, compare, equal]

    let compare p p' = compare p p' |> Int.neg
  end)
end

(*
  include Price

  let of_price (price : Price.t) : t = price

 let compare p p' = Float.compare p p' |> Int.neg
*)
module Ask_price = struct
  include Price

  let of_price (price : Price.t) : t = price
end

module Price_level = struct
  type t =
    { price : Price.t;
      volume : float
    }
  [@@deriving sexp, equal, compare, fields]

  let create ~price ~volume = { price; volume }

  let empty = create ~price:0. ~volume:0.
end

module Bid_price_map = Map.Make_using_comparator (Bid_price)
module Ask_price_map = Map.Make (Ask_price)
module Bid_ask = Market_data.Side.Bid_ask

module Book = struct
  type t =
    { symbol : Symbol.t;
      bids : Price_level.t Bid_price_map.t;
      asks : Price_level.t Ask_price_map.t;
      epoch : int;
      timestamp : Timestamp.t
    }
  [@@deriving fields, compare, equal, sexp]

  let empty ?(epoch = 0) symbol =
    { symbol;
      bids = Bid_price_map.empty;
      asks = Ask_price_map.empty;
      epoch;
      timestamp = Timestamp.now ()
    }

  let set ?(timestamp = Timestamp.now ()) t ~(side : Bid_ask.t) ~price ~size =
    let epoch = t.epoch + 1 in
    match Float.(equal zero size) with
    | true -> (
      match side with
      | `Bid -> { t with bids = Map.remove t.bids price; epoch }
      | `Ask -> { t with asks = Map.remove t.asks price; epoch } )
    | false -> (
      let data = Price_level.create ~price ~volume:size in
      match side with
      | `Bid ->
        { t with bids = Map.set t.bids ~key:price ~data; epoch; timestamp }
      | `Ask -> { t with asks = Map.set t.asks ~key:price ~data; epoch } )

  let update ?(timestamp = Timestamp.now ()) (t : t) ~(side : Bid_ask.t) ~price
      ~size =
    let size_ref = ref 0. in
    let maybe_remove orders =
      match Float.(equal zero !size_ref) with
      | true -> Map.remove orders price
      | false -> orders
    in
    match side with
    | `Bid ->
      { t with
        bids =
          Map.update t.bids price ~f:(function
            | None -> Price_level.create ~price ~volume:size
            | Some { price; volume = size' } ->
              size_ref := size +. size';
              let volume = !size_ref in
              Price_level.create ~price ~volume )
          |> maybe_remove;
        epoch = t.epoch + 1;
        timestamp
      }
    | `Ask ->
      { t with
        asks =
          Map.update t.asks price ~f:(function
            | None -> Price_level.create ~price ~volume:size
            | Some { price; volume = size' } ->
              size_ref := size +. size';
              let volume = !size_ref in
              Price_level.create ~price ~volume )
          |> maybe_remove;
        epoch = t.epoch + 1;
        timestamp
      }

  let add ?timestamp t ~side ~price ~size =
    if Float.is_negative size then
      failwithf "Only positive integers expected: %f" size ()
    else
      update ?timestamp t ~side ~price ~size

  let remove ?timestamp t ~side ~price ~size =
    if Float.is_negative size then
      failwithf "Only positive integers expected: %f" size ()
    else
      update ?timestamp t ~side ~price ~size:(-.size)

  let best_bid t =
    Map.min_elt t.bids |> Option.value ~default:(0., Price_level.empty) |> snd

  let best_ask t =
    Map.min_elt t.asks |> Option.value ~default:(0., Price_level.empty) |> snd

  let bid_market_price t ~volume =
    let normalize Price_level.{ price; volume } =
      Price_level.create ~price:(price /. volume) ~volume
    in
    Map.fold_until ~init:Price_level.empty t.bids ~finish:Fn.id
      ~f:(fun
          ~key:price
          ~data:{ price = _; volume = size }
          Price_level.{ price = avg_price; volume = total_size }
        ->
        let total_size' = Float.min volume (size +. total_size) in
        let size = total_size' -. total_size in
        let price_level =
          Price_level.create
            ~price:(avg_price +. (price *. size))
            ~volume:total_size'
        in
        match Float.( >= ) total_size volume with
        | true -> Continue_or_stop.Stop price_level
        | false -> Continue_or_stop.Continue price_level )
    |> normalize

  let ask_market_price t ~volume =
    let normalize Price_level.{ price; volume } =
      Price_level.create ~price:(price /. volume) ~volume
    in
    Map.fold_until ~init:Price_level.empty t.asks ~finish:Fn.id
      ~f:(fun
          ~key:price
          ~data:{ price = _; volume = size }
          Price_level.{ price = avg_price; volume = total_size }
        ->
        let total_size' = Float.min volume (size +. total_size) in
        let size = total_size' -. total_size in
        let price_level =
          Price_level.create
            ~price:(avg_price +. (price *. size))
            ~volume:total_size'
        in
        match Float.( >= ) total_size volume with
        | true -> Continue_or_stop.Stop price_level
        | false -> Continue_or_stop.Continue price_level )
    |> normalize

  let market_price t ~side ~volume =
    match side with
    | `Bid
    | `Buy ->
      bid_market_price t ~volume
    | `Ask
    | `Sell ->
      ask_market_price t ~volume

  let total_bid_volume_at_price_level t ~price =
    let orders, min, max =
      ( t.bids,
        price,
        Map.max_elt t.bids |> Option.value_map ~f:fst ~default:price )
    in
    Map.fold_range_inclusive orders ~init:(0., 0.) ~max ~min
      ~f:(fun
          ~key:price
          ~data:{ price = _; volume = size }
          (avg_price, total_size)
        ->
        let price = Float.abs price in
        (avg_price +. (price *. size), size +. total_size) )
    |> fun (avg_price, total_size) ->
    Price_level.{ price = avg_price /. total_size; volume = total_size }

  let total_ask_volume_at_price_level t ~price =
    let orders, min, max =
      ( t.asks,
        price,
        Map.min_elt t.asks |> Option.value_map ~f:fst ~default:price )
    in
    Map.fold_range_inclusive orders ~init:(0., 0.) ~max ~min
      ~f:(fun
          ~key:price
          ~data:{ price = _; volume = size }
          (avg_price, total_size)
        ->
        let price = Float.abs price in
        (avg_price +. (price *. size), size +. total_size) )
    |> fun (avg_price, total_size) ->
    Price_level.{ price = avg_price /. total_size; volume = total_size }

  let total_volume_at_price_level t ~side ~price =
    match side with
    | `Bid -> total_bid_volume_at_price_level t ~price
    | `Ask -> total_ask_volume_at_price_level t ~price

  let total_bid_volume_at_price_level t ~price =
    total_volume_at_price_level t ~side:`Bid ~price

  let total_ask_volume_at_price_level t ~price =
    total_volume_at_price_level t ~side:`Ask ~price

  let best ~side =
    match side with
    | `Bid -> best_bid
    | `Ask -> best_ask

  let best_n_bids t ~n () =
    let bids = Map.to_alist ~key_order:`Increasing t.bids in
    List.chunks_of ~length:n bids
    |> List.hd |> Option.value ~default:[] |> List.map ~f:snd |> List.rev

  let best_n_asks t ~n () =
    let asks = Map.to_alist ~key_order:`Increasing t.asks in
    List.chunks_of ~length:n asks
    |> List.hd |> Option.value ~default:[] |> List.map ~f:snd

  let on_market_data t (market_data : Market_data.response) =
    let message = market_data.message in
    let f t (event : Market_data.event) =
      match event with
      | `Auction_open _auction_open -> t
      | `Change { price; side; reason = _; remaining; delta = _ } ->
        let size = Float.of_string remaining in
        let price = Float.of_string price in
        set t ~price ~size ~side
      | `Trade _trade -> t
      | `Block_trade _trade -> t
      | `Auction _auction -> t
    in

    let t =
      match message with
      | `Heartbeat () -> t
      | `Update { event_id = _; events; timestamp = _; timestampms = _ } ->
        Array.fold events ~init:t ~f
    in
    (*print_s (sexp_of_t t);*)
    t

  let pretty_print ?(max_depth = 10) t =
    let padding n =
      List.range 0 n
      |> List.fold ~f:(fun x (_ : int) -> String.concat [ "."; x ]) ~init:""
    in
    printf "#### %s Orderbook (%d) ####\n" (Symbol.to_string t.symbol) t.epoch;
    printf "---------------------------\n";
    let empirical_padding = ref "" in
    let printer ~side ~price ~size =
      let price = Float.abs price in
      let size_at_price_str = sprintf "%f @ %f " size price in
      let padding = padding @@ String.length size_at_price_str in
      empirical_padding := padding;
      match side with
      | `Bid -> printf "%s | %s\n" size_at_price_str padding
      | `Ask -> printf "%s | %s\n" padding size_at_price_str
    in
    let bids = best_n_bids t ~n:max_depth () in
    List.iter bids ~f:(fun Price_level.{ price; volume = size } ->
        printer ~side:`Bid ~price ~size );
    printf "%s | %s\n" !empirical_padding !empirical_padding;
    let asks = best_n_asks t ~n:max_depth () in
    List.iter asks ~f:(fun Price_level.{ price; volume = size } ->
        printer ~side:`Ask ~price ~size )

  let pipe (module Cfg : Cfg.S) ~symbol () =
    let book = empty symbol in
    Market_data.client (module Cfg) ?query:None ~uri_args:symbol ()
    >>| fun pipe ->
    Pipe.folding_map ~init:book pipe ~f:(fun book response ->
        match response with
        | `Ok response ->
          let book = on_market_data book response in
          (book, `Ok book)
        | #Market_data.Error.t as e -> (book, e) )

  let pipe_exn (module Cfg : Cfg.S) ~symbol () =
    pipe (module Cfg) ~symbol ()
    >>| Pipe.map ~f:(function
          | `Ok x -> x
          | #Market_data.Error.t as e ->
            failwiths ~here:[%here] "Market data error" e
              Market_data.Error.sexp_of_t )
end

module Books = struct
  type t = { books : Book.t Symbol_map.t }
  [@@deriving fields, compare, equal, sexp]

  let empty = { books = Symbol_map.empty }

  let update_ ~f t ~symbol ~side ~price ~size =
    let books =
      Map.update t.books symbol ~f:(function
        | None ->
          let book = Book.empty symbol in
          f book ~side ~price ~size
        | Some book -> f book ~side ~price ~size )
    in
    { books }

  let add ?timestamp = update_ ~f:(Book.add ?timestamp)

  let update ?timestamp = update_ ~f:(Book.update ?timestamp)

  let remove ?timestamp = update_ ~f:(Book.remove ?timestamp)

  let set ?timestamp = update_ ~f:(Book.set ?timestamp)

  let symbols t = Map.keys t.books

  let book (t : t) symbol = Map.find t.books symbol

  let set_book (t : t) (book : Book.t) =
    let books = t.books in
    Map.set books ~key:book.symbol ~data:book |> fun books -> { books }

  let book_exn (t : t) symbol = Map.find_exn t.books symbol

  let on_market_data t symbol (market_data : Market_data.response) =
    Map.update t.books symbol ~f:(function
      | None -> Book.on_market_data (Book.empty symbol) market_data
      | Some book -> Book.on_market_data book market_data )

  let pipe (module Cfg : Cfg.S) ?(symbols = Symbol.all) () :
      [ `Ok of Book.t | Market_data.Error.t ] Pipe.Reader.t Symbol_map.t
      Deferred.t =
    Deferred.List.map ~how:`Parallel symbols ~f:(fun symbol ->
        Deferred.both (return symbol) (Book.pipe (module Cfg) ~symbol ()) )
    >>| Symbol_map.of_alist_exn

  let pipe_exn (module Cfg : Cfg.S) ?symbols () :
      Book.t Pipe.Reader.t Symbol_map.t Deferred.t =
    pipe (module Cfg) ?symbols ()
    >>| Symbol_map.map ~f:(fun pipe ->
            Pipe.map pipe ~f:(fun result ->
                match result with
                | `Ok x -> x
                | #Market_data.Error.t as e ->
                  failwiths "Errror updating book" ~here:[%here] e
                    Market_data.Error.sexp_of_t ) )
end

let command =
  let spec : (_, _) Command.Spec.t =
    let open Command.Spec in
    empty +> Cfg.param
    +> flag "-loglevel" (optional int) ~doc:"1-3 loglevel"
    +> flag "-csv-dir" (optional string)
         ~doc:
           "PATH output each event type to a separate csv file at PATH. \
            Defaults to current directory."
    +> Command.Spec.flag "--no-csv" no_arg ~doc:"Disable csv generation."
    +> anon (maybe ("uri_args" %: sexp))
  in
  let set_loglevel = function
    | 2 ->
      Log.Global.set_level `Info;
      Logs.set_level @@ Some Logs.Info
    | e when e > 2 ->
      Log.Global.set_level `Debug;
      Logs.set_level @@ Some Logs.Debug
    | _ -> ()
  in
  let run cfg loglevel (_csv_dir : string option) _no_csv symbol () =
    let cfg = Cfg.or_default cfg in
    let module Cfg = (val cfg : Cfg.S) in
    Option.iter loglevel ~f:set_loglevel;
    let symbol =
      Option.first_some
        (Option.map ~f:Market_data.uri_args_of_sexp symbol)
        Market_data.default_uri_args
      |> Option.value ~default:`Ethusd
    in
    Book.pipe (module Cfg) ~symbol ()
    >>= Pipe.iter ~f:(function
          | `Ok book ->
            Book.pretty_print book;
            Deferred.unit
          | #Market_data.Error.t as e ->
            failwiths ~here:[%here] "Market data error" e
              Market_data.Error.sexp_of_t )
  in
  ( "orderbook",
    Command.async_spec
      ~summary:
        (sprintf "Gemini %s %s Orderbook Command" Market_data.version
           "orderbook" )
      spec run )
