open! Common
open V1

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
      update_time : Timestamp.t
    }
  [@@deriving fields, compare, equal, sexp]

  let empty ?timestamp ?(epoch = 0) symbol =
    let update_time = Option.value_or_thunk timestamp ~default:Timestamp.now in
    { symbol;
      bids = Bid_price_map.empty;
      asks = Ask_price_map.empty;
      epoch;
      update_time
    }

  let set ?timestamp t ~(side : Bid_ask.t) ~price ~size =
    let update_time = Option.value_or_thunk timestamp ~default:Timestamp.now in
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
        { t with bids = Map.set t.bids ~key:price ~data; epoch; update_time }
      | `Ask ->
        { t with asks = Map.set t.asks ~key:price ~data; epoch; update_time } )

  let update ?timestamp (t : t) ~(side : Bid_ask.t) ~price ~size =
    let update_time = Option.value_or_thunk timestamp ~default:Timestamp.now in
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
        update_time
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
        update_time
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

  (** Convert notional volume (USD) to base asset quantity using best bid/ask *)
  let quantity_from_notional_bid t ~notional =
    let Price_level.{ price = bid_price; _ } = best_bid t in
    if Float.(bid_price <= 0.) then notional else notional /. bid_price

  let quantity_from_notional_ask t ~notional =
    let Price_level.{ price = ask_price; _ } = best_ask t in
    if Float.(ask_price <= 0.) then notional else notional /. ask_price

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

  (* Mutable state for throttling renders per symbol *)
  let last_render_times : float Symbol.Map.t ref = ref Symbol.Map.empty

  (* Render a live TUI for the order book using ANSI escapes.
     - Draws two columns: bids (left, green) and asks (right, red)
     - Scales bar lengths to terminal width and max volume within visible depth
     - Shows spread and mid in header; clears and redraws each update.
     - Optional throttle and aggregation to slow down visual updates. *)
  let pretty_print ?(max_depth = 12) ?(refresh_ms = 100.) ?(tick_size = None) t =
    (* Throttle: skip render if less than refresh_ms elapsed since last. *)
    let now_ms = Time_float_unix.now () |> Time_float_unix.to_span_since_epoch
                 |> Time_float_unix.Span.to_ms in
    let last = Map.find !last_render_times t.symbol in
    let should_render =
      match last with
      | None -> true
      | Some last_ms ->
        Float.(>=) (now_ms -. last_ms) refresh_ms
    in
    if not should_render then ()
    else begin
      last_render_times := Map.set !last_render_times ~key:t.symbol ~data:now_ms;

      (* Optional tick aggregation: round prices to nearest tick multiple. *)
      let aggregate_levels levels =
        match tick_size with
        | None -> levels
        | Some tick ->
          List.map levels ~f:(fun Price_level.{ price; volume } ->
            let rounded = Float.round (price /. tick) *. tick in
            Price_level.create ~price:rounded ~volume )
          |> List.sort ~compare:(fun a b -> Float.compare a.price b.price)
          |> List.fold ~init:[] ~f:(fun acc lvl ->
              match acc with
              | [] -> [lvl]
              | hd :: tl ->
                if Float.(=) hd.price lvl.price then
                  Price_level.{ price = hd.price; volume = hd.volume +. lvl.volume } :: tl
                else lvl :: acc )
          |> List.rev
      in

    (* ANSI helpers *)
    let esc = "\027[" in
    let ansi code = printf "%s%sm" esc code in
    let reset () = ansi "0" in
    let color dim = function
      | `Bid -> ansi (sprintf "%s;%s" dim "32") (* green *)
      | `Ask -> ansi (sprintf "%s;%s" dim "31") (* red *)
    in

    (* Terminal size (best-effort) *)
    let term_cols =
      Sys.getenv "COLUMNS"
      |> Option.bind ~f:(fun s -> Option.try_with (fun () -> Int.of_string s))
      |> Option.value ~default:100
    in
    let left_col_width = 24 in
    let right_col_width = 24 in
    let padding_between_cols = 3 in
    let total_meta = left_col_width + right_col_width + padding_between_cols in
    let bar_total_width = Int.max 0 (term_cols - total_meta) in
    let bar_width_each = bar_total_width / 2 in

    (* Gather top N levels *)
    let bids = best_n_bids t ~n:max_depth () |> aggregate_levels in
    let asks = best_n_asks t ~n:max_depth () |> aggregate_levels in

    let max_vol =
      let both = Array.append (Array.of_list bids) (Array.of_list asks) in
      Array.fold both ~init:0. ~f:(fun acc Price_level.{ volume; _ } ->
          Float.max acc (Float.abs volume) )
    in
    let max_vol = if Float.(=) max_vol 0. then 1. else max_vol in

    (* Decide price precision based on smallest visible price magnitude. *)
    let visible_prices =
      let gather lst =
        List.filter_map lst ~f:(fun Price_level.{ price; _ } ->
            let p = Float.abs price in
            if Float.(>) p 0. then Some p else None )
      in
      gather bids @ gather asks
    in
    let min_price_mag =
      match visible_prices with
      | [] -> 0.
      | hd :: tl -> List.fold tl ~init:hd ~f:Float.min
    in
    let compute_decimals p =
      if Float.(<=) p 0. then 2
      else if Float.(>=) p 1000. then 0
      else if Float.(>=) p 100. then 1
      else if Float.(>=) p 1. then 2
      else
        let log10 x = Float.log x /. Float.log 10. in
        let d = Stdlib.ceil (-. (log10 p)) |> Int.of_float in
        Int.min 10 (Int.max 2 (d + 2))
    in
    let price_decimals = compute_decimals min_price_mag in
    let format_price ~decimals p =
      let p = Float.abs p in
      sprintf "%12.*f" decimals p
    in
    let format_size s =
      let s = Float.abs s in
      if Float.(s >= 1000.) then sprintf "%10.0f" s
      else if Float.(s >= 100.) then sprintf "%10.1f" s
      else if Float.(s >= 1.) then sprintf "%10.3f" s
      else sprintf "%10.6f" s
    in
    let pad_right w s =
      if String.length s >= w then String.prefix s w
      else s ^ String.make (w - String.length s) ' '
    in
    let pad_left w s =
      if String.length s >= w then String.suffix s w
      else String.make (w - String.length s) ' ' ^ s
    in
    let bar ~width ~side ~volume =
      let frac = Float.min 1. (Float.abs volume /. max_vol) in
      let filled = Int.of_float (Float.round_down (frac *. Float.of_int width)) in
      let empty = Int.max 0 (width - filled) in
  ( match side with
      | `Bid -> color "1" `Bid (* bold green *)
      | `Ask -> color "1" `Ask (* bold red *) );
  printf "%s" (String.make filled '#');
      reset ();
      ansi "2"; (* dim *)
  printf "%s" (String.make empty '.');
      reset ()
    in

    (* Compute best, spread, mid *)
    let Price_level.{ price = best_bid_price; _ } = best_bid t in
    let Price_level.{ price = best_ask_price; _ } = best_ask t in
    let spread =
      if Float.(=) best_bid_price 0. || Float.(=) best_ask_price 0. then Float.nan
      else best_ask_price -. best_bid_price
    in
    let mid =
      if Float.(=) best_bid_price 0. || Float.(=) best_ask_price 0. then Float.nan
      else (best_ask_price +. best_bid_price) /. 2.
    in

    (* Clear screen and home cursor, then draw *)
    printf "\027[2J\027[H";

    (* Header line *)
    let sym = Symbol.to_string t.symbol |> String.uppercase in
    ansi "1;37"; (* bright white *)
    printf " %s Order Book  " sym;
    reset ();
    ansi "2"; (* dim *)
    printf "epoch=%d  levels=%d  " t.epoch max_depth;
    ( match Float.is_nan spread with
    | true -> ()
    | false ->
      printf "spread=$%s  mid=$%s"
        (format_price ~decimals:price_decimals spread)
        (format_price ~decimals:price_decimals mid) );
    reset ();
    printf "\n";

    (* Subheader *)
    ansi "2";
    printf "%s\n" (String.make (Int.max 0 (term_cols - 2)) '-');
    reset ();

    (* Column headers *)
  color "0" `Bid;
  printf "%s" (pad_right left_col_width "Bids (size @ price)");
    reset ();
    printf "%s" (String.make bar_width_each ' ');
    printf "%s" (String.make padding_between_cols ' ');
    printf "%s" (String.make bar_width_each ' ');
  color "0" `Ask;
  printf "%s\n" (pad_left right_col_width "Asks (price @ size)");
    reset ();

    (* Rows *)
    let rows = Int.max (List.length bids) (List.length asks) in
    let rec draw i bids asks =
      match (i, bids, asks) with
      | n, _, _ when n >= rows -> ()
      | n, (b :: bt), (a :: at) ->
        let Price_level.{ price = b_price; volume = b_volume } = b in
        let Price_level.{ price = a_price; volume = a_volume } = a in
        (* Left: bid text *)
        let btxt =
          sprintf "%s @ %s" (format_size b_volume)
            (format_price ~decimals:price_decimals b_price)
          |> pad_right left_col_width
        in
        color "0" `Bid;
        printf "%s" btxt;
        reset ();
        (* Left bar for bids *)
        bar ~width:bar_width_each ~side:`Bid ~volume:b_volume;
        (* Middle gap *)
        printf "%s" (String.make padding_between_cols ' ');
        (* Right bar for asks *)
        bar ~width:bar_width_each ~side:`Ask ~volume:a_volume;
        (* Right: ask text (mirrored order: price @ size) *)
        let atxt =
          sprintf "%s @ %s"
            (format_price ~decimals:price_decimals a_price)
            (format_size a_volume)
          |> pad_left right_col_width
        in
        color "0" `Ask;
        printf "%s\n" atxt;
        reset ();
        draw (n + 1) bt at
      | n, (b :: bt), [] ->
        let Price_level.{ price = b_price; volume = b_volume } = b in
        let btxt =
          sprintf "%s @ %s" (format_size b_volume)
            (format_price ~decimals:price_decimals b_price)
          |> pad_right left_col_width
        in
        color "0" `Bid;
        printf "%s" btxt;
        reset ();
        bar ~width:bar_width_each ~side:`Bid ~volume:b_volume;
        printf "%s" (String.make padding_between_cols ' ');
        printf "%s" (String.make bar_width_each ' ');
        printf "%s\n" (String.make right_col_width ' ');
        draw (n + 1) bt []
      | n, [], (a :: at) ->
        let Price_level.{ price = a_price; volume = a_volume } = a in
        printf "%s" (String.make left_col_width ' ');
        printf "%s" (String.make bar_width_each ' ');
        printf "%s" (String.make padding_between_cols ' ');
        bar ~width:bar_width_each ~side:`Ask ~volume:a_volume;
        let atxt =
          sprintf "%s @ %s"
            (format_price ~decimals:price_decimals a_price)
            (format_size a_volume)
          |> pad_left right_col_width
        in
        color "0" `Ask;
        printf "%s\n" atxt;
        reset ();
        draw (n + 1) [] at
      | _ -> ()
    in
    draw 0 bids asks;

    (* Footer guide *)
    ansi "2";
    printf "%s\n" (String.make (Int.max 0 (term_cols - 2)) '-');
    reset ()
    end

  let pipe (module Cfg : Cfg.S) ~(symbol : Symbol.t) () =
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
  type t =
    { books : Book.t Symbol.Map.t;
      update_time : Timestamp.t
    }
  [@@deriving fields, compare, equal, sexp]

  let empty = { books = Symbol.Map.empty; update_time = Timestamp.now () }

  let update_ ?timestamp ~f t ~symbol ~side ~price ~size =
    let timestamp = Option.value_or_thunk timestamp ~default:Timestamp.now in
    let books =
      Map.update t.books symbol ~f:(function
        | None ->
          let book = Book.empty ~timestamp symbol in
          f book ~side ~price ~size
        | Some book -> f book ~side ~price ~size )
    in
    { books; update_time = timestamp }

  let add ?timestamp = update_ ~f:(Book.add ?timestamp)

  let update ?timestamp = update_ ~f:(Book.update ?timestamp)

  let remove ?timestamp = update_ ~f:(Book.remove ?timestamp)

  let set ?timestamp = update_ ~f:(Book.set ?timestamp)

  let symbols t = Map.keys t.books

  let book (t : t) symbol = Map.find t.books symbol

  let set_book ?timestamp (t : t) (book : Book.t) =
    let update_time = Option.value_or_thunk timestamp ~default:Timestamp.now in
    let books = t.books in
    Map.set books ~key:book.symbol ~data:book |> fun books ->
    { books; update_time }

  let book_exn (t : t) symbol = Map.find_exn t.books symbol

  let on_market_data t symbol (market_data : Market_data.response) =
    Map.update t.books symbol ~f:(function
      | None -> Book.on_market_data (Book.empty symbol) market_data
      | Some book -> Book.on_market_data book market_data )

  let pipe (module Cfg : Cfg.S) ?(symbols = Symbol.all) () :
      [ `Ok of Book.t | Market_data.Error.t ] Pipe.Reader.t Symbol.Map.t
      Deferred.t =
    Deferred.List.map ~how:`Parallel symbols ~f:(fun symbol ->
        Deferred.both (return symbol) (Book.pipe (module Cfg) ~symbol ()) )
    >>| Symbol.Map.of_alist_exn

  let pipe_exn (module Cfg : Cfg.S) ?symbols () :
      Book.t Pipe.Reader.t Symbol.Map.t Deferred.t =
    pipe (module Cfg) ?symbols ()
    >>| Symbol.Map.map ~f:(fun pipe ->
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
    +> flag "--refresh-ms" (optional_with_default 100. float)
         ~doc:"MS minimum milliseconds between screen redraws (default: 100)"
    +> flag "--tick-size" (optional float)
         ~doc:"TICK aggregate price levels to nearest tick multiple (e.g., 0.01, 1.0)"
    +> flag "--max-depth" (optional_with_default 12 int)
         ~doc:"N maximum number of bid/ask levels to display (default: 12)"
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
  let run cfg loglevel (_csv_dir : string option) _no_csv refresh_ms tick_size max_depth symbol () =
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
            Book.pretty_print ~max_depth ~refresh_ms ~tick_size book;
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
