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
module Price_level = struct
  type t =
    { price : float;
      volume : float
    }
  [@@deriving sexp, equal, compare, fields]

  let create ~price ~volume = { price; volume }

  let empty = create ~price:0. ~volume:0.
end

module Bid_price_map = Map.Make (Float)
module Ask_price_map = Map.Make (Float)
module Bid_ask = Market_data.Side.Bid_ask

module Book = struct
  type t =
    { symbol : Symbol.t;
      bids : float Bid_price_map.t;
      asks : float Ask_price_map.t;
      epoch : int
    }
  [@@deriving fields, compare, equal, sexp]

  let empty ?(epoch = 0) symbol =
    { symbol; bids = Bid_price_map.empty; asks = Ask_price_map.empty; epoch }

  let signed_price side price =
    match side with
    | `Bid -> Float.neg price
    | `Ask -> price

  let set (t : t) ~(side : Bid_ask.t) ~price ~size =
    let epoch = t.epoch + 1 in
    let price = signed_price side price in
    match Float.(equal zero size) with
    | true -> (
      match side with
      | `Bid -> { t with bids = Map.remove t.bids price; epoch }
      | `Ask -> { t with asks = Map.remove t.asks price; epoch } )
    | false -> (
      match side with
      | `Bid -> { t with bids = Map.set t.bids ~key:price ~data:size; epoch }
      | `Ask -> { t with asks = Map.set t.asks ~key:price ~data:size; epoch } )

  let update (t : t) ~(side : Bid_ask.t) ~price ~size =
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
            | None -> size
            | Some size' ->
              size_ref := size +. size';
              !size_ref )
          |> maybe_remove;
        epoch = t.epoch + 1
      }
    | `Ask ->
      { t with
        bids =
          Map.update t.asks price ~f:(function
            | None -> size
            | Some size' ->
              size_ref := size +. size';
              !size_ref )
          |> maybe_remove;
        epoch = t.epoch + 1
      }

  let add t ~side ~price ~size =
    if Float.is_negative size then
      failwith "Only positive integers expected"
    else
      update t ~side ~price ~size

  let remove t ~side ~price ~size =
    if Float.is_negative size then
      failwith "Only positive integers expected"
    else
      update t ~side ~price ~size:(-.size)

  let best_bid t =
    Map.min_elt t.bids
    |> Option.value_map
         ~f:(fun (price, size) -> (signed_price `Bid price, size))
         ~default:(Float.nan, 0.)

  let best_ask t = Map.min_elt t.asks |> Option.value ~default:(Float.nan, 0.)

  let market_price t ~side ~volume =
    let orders =
      match side with
      | `Bid -> t.bids
      | `Ask -> t.asks
    in
    let normalize Price_level.{ price; volume } =
      Price_level.create ~price:(price /. volume) ~volume
    in
    Map.fold_until ~init:Price_level.empty orders ~finish:Fn.id
      ~f:(fun
          ~key:price
          ~data:size
          Price_level.{ price = avg_price; volume = total_size }
        ->
        let total_size' = Float.min volume (size +. total_size) in
        let price = Float.abs price in
        match Float.equal total_size volume with
        | true ->
          Continue_or_stop.Stop
            (Price_level.create
               ~price:(avg_price +. (price *. (total_size' -. total_size)))
               ~volume:total_size' )
        | false ->
          Continue_or_stop.Continue
            (Price_level.create
               ~price:(avg_price +. (price *. size))
               ~volume:total_size' ) )
    |> normalize

  let bid_market_price t = market_price t ~side:`Bid

  let ask_market_price t = market_price t ~side:`Ask

  let total_volume_at_price_level t ~side ~price =
    let price = signed_price side price in
    let orders, min, max =
      match side with
      | `Bid ->
        ( t.bids,
          price,
          Map.max_elt t.bids |> Option.value_map ~f:fst ~default:price )
      | `Ask ->
        ( t.asks,
          price,
          Map.min_elt t.asks |> Option.value_map ~f:fst ~default:price )
    in
    Map.fold_range_inclusive orders ~init:(0., 0.) ~max ~min
      ~f:(fun ~key:price ~data:size (avg_price, total_size) ->
        let price = Float.abs price in
        (avg_price +. (price *. size), size +. total_size) )
    |> fun (avg_price, total_size) ->
    Price_level.{ price = avg_price /. total_size; volume = total_size }

  let total_bid_volume_at_price_level t ~price =
    total_volume_at_price_level t ~side:`Bid ~price

  let total_ask_volume_at_price_level t ~price =
    total_volume_at_price_level t ~side:`Ask ~price

  let best ~side =
    match side with
    | `Bid -> best_bid
    | `Ask -> best_ask

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

  let pretty_print ?(max_depth = 40) t =
    let padding = "......................." in
    printf "#### %s Orderbook (%d) ####\n" (Symbol.to_string t.symbol) t.epoch;
    printf "---------------------------\n";
    let printer ~side ~price ~size =
      let price = Float.abs price in
      match side with
      | `Bid -> sprintf "%f @ %f | %s" size price padding
      | `Ask -> sprintf "%s | %f @ %f" padding size price
    in
    let print_all ~reverse l =
      ( match reverse with
      | false -> List.rev l
      | true -> l )
      |> List.iter ~f:(printf "%s\n")
    in
    let f ~side ~key:price ~data:size acc =
      match List.length acc < max_depth with
      | true ->
        let s = printer ~side ~price ~size in
        Continue_or_stop.Continue (s :: acc)
      | false ->
        Continue_or_stop.Stop
          (print_all
             ~reverse:
               ( match side with
               | `Bid -> true
               | `Ask -> false )
             acc )
    in
    Map.fold_until ~init:[] t.bids ~finish:(fun _ -> ()) ~f:(f ~side:`Bid);
    printf "%s | %s\n" padding padding;
    Map.fold_until ~init:[] t.asks ~finish:(fun _ -> ()) ~f:(f ~side:`Ask)

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
end

module Books = struct
  type t = { books : Book.t Symbol_map.t }
  [@@deriving fields, compare, equal, sexp]

  let update_ ~f t ~symbol ~side ~price ~size =
    let books =
      Map.update t.books symbol ~f:(function
        | None ->
          let book = Book.empty symbol in
          f book ~side ~price ~size
        | Some book -> f book ~side ~price ~size )
    in
    { books }

  let add = update_ ~f:Book.add

  let update = update_ ~f:Book.update

  let remove = update_ ~f:Book.remove

  let set = update_ ~f:Book.set

  let symbols t = Map.keys t.books

  let book (t : t) symbol = Map.find t.books symbol

  let book_exn (t : t) symbol = Map.find_exn t.books symbol

  let on_market_data t symbol (market_data : Market_data.response) =
    Map.update t.books symbol ~f:(function
      | None -> Book.on_market_data (Book.empty symbol) market_data
      | Some book -> Book.on_market_data book market_data )

  (* let market_value ?(symbols = Symbol.all) ~side (t : t) () =
     Map.filter_mapi t.books ~f:(fun ~key:symbol ~data:book ->

         List.find_map symbols ~f:(fun symbol' ->
             match Symbol.equal symbol symbol' with
             | false -> None
             | true -> Book.market_price ~side book ~volume ) )*)
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
