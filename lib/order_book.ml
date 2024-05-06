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

  let set (t : t) ~(side : Bid_ask.t) ~price ~size =
    let epoch = t.epoch + 1 in
    let price =
      match side with
      | `Bid -> Float.neg price
      | `Ask -> price
    in
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

  let best_bid t = Map.min_elt t.bids |> Option.value ~default:(Float.nan, 0.)

  let best_ask t = Map.min_elt t.asks |> Option.value ~default:(Float.nan, 0.)

  let best ~side =
    match side with
    | `Buy -> best_bid
    | `Sell -> best_ask

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

  let pretty_print ?(max_depth = 20) t =
    let padding = "..........." in
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

  let on_market_data t symbol (market_data : Market_data.response) =
    Map.update t.books symbol ~f:(function
      | None -> Book.on_market_data (Book.empty symbol) market_data
      | Some book -> Book.on_market_data book market_data )
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
    let book = Book.empty symbol in
    Market_data.client (module Cfg) ?query:None ~uri_args:symbol ()
    >>= fun pipe ->
    Pipe.folding_map ~init:book pipe ~f:(fun book response ->
        match response with
        | `Ok response ->
          let book = Book.on_market_data book response in
          (book, `Ok book)
        | #Market_data.Error.t as e -> (book, e) )
    |> Pipe.iter ~f:(function
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
