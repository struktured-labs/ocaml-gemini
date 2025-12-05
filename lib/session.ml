open Core
open Async
open V1
module Symbol = Symbol
module Order_id = Int64
module Order_map = Map.Make (Order_id)
module Error = Rest.Error
module Concurrent_order_map = Concurrent_map.Make (Int64)

module Client_order_id = struct
  include Nonce.File

  let default_filename = "~/.gemini/client_order_id"

  type reader = string Inf_pipe.Reader.t

  let pipe ~prefix () =
    pipe ~init:default_filename ()
    >>| Inf_pipe.map ~f:(fun i -> Format.sprintf "%s-%d" prefix i)
end

type status_pipe =
  [ `Ok of Order.Status.response | Error.post ] Inf_pipe.Reader.t

let status_pipe cfg nonce order_id : status_pipe =
  let request = Order.Status.{ order_id } in
  Inf_pipe.unfold ~init:0 ~f:(fun epoch ->
      Order.Status.post cfg nonce request >>| fun response ->
      (response, epoch + 1) )

let trade_pipe cfg nonce symbol =
  let request = Mytrades.{ symbol; limit_trades = None; timestamp = None } in
  Inf_pipe.unfold ~init:0 ~f:(fun epoch ->
      Mytrades.post cfg nonce request >>| fun response -> (response, epoch + 1) )

let balance_pipe cfg nonce =
  let request = () in
  Inf_pipe.unfold ~init:0 ~f:(fun epoch ->
      Balances.post cfg nonce request >>| fun response -> (response, epoch + 1) )

module Events = struct
  (** Auto-restarting pipe wrapper that reconnects on EOF *)
  let auto_restart_pipe ~name ~create_pipe () =
    let reader, writer = Pipe.create () in
    let rec restart_loop () =
      Log.Global.info "auto_restart_pipe[%s]: connecting..." name;
      create_pipe () >>= fun source_pipe ->
      Log.Global.info "auto_restart_pipe[%s]: connected, relaying" name;
      Pipe.transfer source_pipe writer ~f:Fn.id >>= fun () ->
      Log.Global.info "auto_restart_pipe[%s]: EOF detected, restarting in 1s" name;
      after (Time_float_unix.Span.of_sec 1.0) >>= fun () ->
      restart_loop ()
    in
    don't_wait_for (restart_loop ());
    reader

  type t =
    { balance :
        [ `Ok of Balances.response | Rest.Error.post ] Inf_pipe.Reader.t;
      trades :
        [ `Ok of Mytrades.response | Rest.Error.post ] Inf_pipe.Reader.t
        Symbol.Map.t;
      order_status :
        [ `Ok of Order.Status.response | Rest.Error.post ] Inf_pipe.Reader.t
        Concurrent_order_map.t;
      order_events :
        [ `Ok of Order_events.response | Order_events.Error.t ] Pipe.Reader.t;
      market_data : Market_data.event Inf_pipe.Reader.t Symbol.Map.t;
      ledger : Ledger.Entry.t Pipe.Reader.t Symbol.Map.t;
      order_books :
        [ `Ok of Order_book.Book.t | Market_data.Error.t ] Pipe.Reader.t
        Symbol.Map.t;
      symbol_details : Symbol_details.response Symbol.Map.t
    }

  let create ?(symbols : Symbol.t list = []) ?(order_ids : Order_id.t list = [])
      cfg nonce : t Deferred.t =
    let symbols_len = List.length symbols in
    let order_ids_len = List.length order_ids in
    Log.Global.info "Events.create: start symbols=%d order_ids=%d" symbols_len
      order_ids_len;

    let balance =
      Log.Global.debug "Events.create: starting balance_pipe";
      balance_pipe cfg nonce
    in

    let trades =
      Log.Global.debug "Events.create: starting trade_pipes for symbols";
      List.map symbols ~f:(fun symbol ->
          Log.Global.debug "Events.create: trade_pipe %s"
            (Symbol.to_string symbol);
          (symbol, trade_pipe cfg nonce symbol) )
      |> Symbol.Map.of_alist_exn
    in

    let order_status =
      Log.Global.debug "Events.create: starting status_pipes for order_ids";
      List.map order_ids ~f:(fun order_id ->
          Log.Global.debug "Events.create: status_pipe %Ld" order_id;
          (order_id, status_pipe cfg nonce order_id) )
      |> Order_map.of_alist_exn |> Concurrent_order_map.of_map
    in

    Log.Global.debug "Events.create: wiring market_data for %d symbols"
      symbols_len;
    let market_data =
      Symbol.Map.of_alist_exn (List.map symbols ~f:(fun symbol ->
        let market_data_name = sprintf "market_data[%s]" (Symbol.to_string symbol) in
        let market_data_pipe = auto_restart_pipe ~name:market_data_name ~create_pipe:(fun () ->
          Log.Global.info "auto_restart[%s]: connecting Market_data.client" market_data_name;
          Market_data.client cfg ?query:None ~uri_args:symbol () >>| fun pipe ->
          let event_pipe = Pipe.filter_map pipe ~f:(function
            | `Ok { Market_data.message = `Update { events; _ }; _ } -> Some events
            | `Ok { message = `Heartbeat _; _ } -> None
            | #Market_data.Error.t as e ->
              Log.Global.error_s (Market_data.Error.sexp_of_t e);
              None )
          in
          let reader, writer = Pipe.create () in
          don't_wait_for (
            Pipe.iter event_pipe ~f:(fun events ->
              Pipe.transfer_in writer ~from:(Queue.of_array events)
            ) >>| fun () ->
            Pipe.close writer
          );
          reader
        ) () in
        (symbol, Inf_pipe.Reader.create market_data_pipe)
      ))
    in
    Log.Global.debug "Events.create: market_data readers created with auto-restart";
    Log.Global.info "Events.create: starting order_books.pipe";
    let order_books =
      Symbol.Map.of_alist_exn (List.map symbols ~f:(fun symbol ->
        let book_name = sprintf "order_book[%s]" (Symbol.to_string symbol) in
        let book_pipe = auto_restart_pipe ~name:book_name ~create_pipe:(fun () ->
          Order_book.Books.pipe cfg ~symbols:[symbol] () >>| fun books_map ->
          Map.find_exn books_map symbol
        ) () in
        (symbol, book_pipe)
      ))
    in
    Log.Global.info "Events.create: order_books.pipe ready with auto-restart";
    Log.Global.info "Events.create: starting order_events.client";
    let order_events = auto_restart_pipe ~name:"order_events" ~create_pipe:(fun () ->
      Order_events.client cfg ~nonce ?query:None ?uri_args:None ()
    ) () in
    Log.Global.info "Events.create: order_events.client ready with auto-restart";
    let init = Symbol.Map.of_alist_exn (List.map symbols ~f:(fun symbol -> (symbol, Ledger.Entry.create ~symbol:(Symbol.Enum_or_string.of_enum symbol) ()))) in
    let order_events, order_events_exn =
      Pipe.fork ~pushback_uses:`Fast_consumer_only order_events
    in
    Log.Global.debug "Events.create: forked order_events";
    let order_books_fork =
      Map.map ~f:(Pipe.fork ~pushback_uses:`Fast_consumer_only) order_books
    in
    let order_books, order_books_exn =
      (Map.map order_books_fork ~f:fst, Map.map order_books_fork ~f:snd)
    in
    Log.Global.debug "Events.create: forked order_books";
    let order_books_exn =
      Map.map ~f:(Pipe.map ~f:
      (Poly_ok.ok_exn ?sexp_of_error:None ~here:[%here] ~message:"Order books error")) order_books_exn
    in
    let order_events_exn = Pipe.map ~f:
      (Poly_ok.ok_exn ?sexp_of_error:None ~here:[%here] ~message:"Order events error") order_events_exn in
    Log.Global.info "Events.create: starting Ledger.pipe";
    Ledger.pipe ?num_values:None ?how:None ?behavior:None ~init order_books_exn
      order_events_exn
    >>= fun ledger ->
    Log.Global.info "Events.create: finished Ledger.pipe, fetching symbol details";
    Deferred.List.map symbols ~how:`Parallel ~f:(fun symbol ->
        Symbol_details.get cfg nonce ~uri_args:symbol () >>| function
        | `Ok details -> Some (symbol, details)
        | #Rest.Error.get as error ->
            Log.Global.error "Failed to fetch symbol details for %s: %s"
              (Symbol.to_string symbol) (Rest.Error.sexp_of_get error |> Sexp.to_string);
            None)
    >>| fun details_list ->
    let symbol_details =
      List.filter_map details_list ~f:Fn.id |> Symbol.Map.of_alist_exn
    in
    Log.Global.info "Events.create: fetched %d symbol details, assembling record"
      (Map.length symbol_details);
    { balance;
      trades;
      order_status;
      order_events;
      market_data;
      order_books;
      ledger;
      symbol_details
    }

  let balance (t : t) = t.balance

  let trade (t : t) = t.trades

  let market_data (t : t) = t.market_data

  let order_events (t : t) = t.order_events
end

let calc_decimal_places_from value =
  match Float.(value >= 1.0) with
  | true -> 0
  | false ->
    let log_val = Float.log10 value in
    Int.of_float (Float.abs (Float.round_down log_val))

module Make (C : Cfg.S) = struct
  type t =
    { name : string;
      session_id : string;
      api_nonce : Nonce.reader;
      client_order_id_nonce : Client_order_id.reader;
      events : Events.t
    }

  (** CSV-serializable state snapshot for persistence *)
  module State_csv = struct
    type t =
      { name : string;
        session_id : string;
        timestamp_iso : string
      }
    [@@deriving sexp, fields, csv]
  end

  let name t = t.name

  let session_id t = t.session_id

  let api_nonce t = t.api_nonce

  let cfg (_t : t) = (module C : Cfg.S)

  let symbol_details t symbol =
    Map.find t.events.symbol_details symbol

  let symbol_details_exn t symbol =
    match symbol_details t symbol with
    | Some details -> details
    | None -> failwithf "No symbol details found for %s" (Symbol.to_string symbol) ()

  let min_order_size t symbol =
    let details = symbol_details t symbol in
    Option.map details ~f:(fun d -> d.min_order_size |> Float.of_string)

  let min_order_size_exn t symbol =
    match min_order_size t symbol with
    | Some size -> size
    | None -> failwithf "No symbol details found for %s" (Symbol.to_string symbol) ()
    
  (* Format a price with the correct precision based on tick_size *)
  let format_price t symbol price : Common.Decimal_string.t =
    match symbol_details t symbol with
    | None ->
        Log.Global.error "No symbol details for %s, using default precision" (Symbol.to_string symbol);
        Float.to_string price
    | Some details ->
        let decimal_places = calc_decimal_places_from details.quote_increment in
        let formatted_price = sprintf "%.*f" decimal_places price in
        Log.Global.debug "Formatting price %.9f for %s with %d decimal places as %s" price (Symbol.to_string symbol) decimal_places formatted_price;
        formatted_price

  (* Format a quantity with the correct precision based on quote_increment *)
  let format_quantity t symbol quantity =
    match symbol_details t symbol with
    | None ->
        Log.Global.error "No symbol details for %s, using default precision" (Symbol.to_string symbol);
        Float.to_string quantity
    | Some details ->
        let decimal_places = calc_decimal_places_from details.tick_size in
        let formatted = sprintf "%.*f" decimal_places quantity in
        Log.Global.debug "Formatting quantity %.9f for %s with %d decimal places as %s" quantity (Symbol.to_string symbol) decimal_places formatted;
        formatted
        
  let to_state_csv (t : t) : State_csv.t =
    { name = t.name; 
      session_id = t.session_id; 
      timestamp_iso = Time_float_unix.to_string_iso8601_basic (Time_float_unix.now ()) ~zone:Time_float_unix.Zone.utc
    }

  let write_state_csv ?(path = ".gemini_session_state.csv") (t : t) : unit Deferred.t =
    let csv_row = to_state_csv t in
    (* Check if file exists to determine if we need to write header *)
    Sys.file_exists path >>= fun exists ->
    Writer.with_file path ~append:true ~f:(fun writer ->
        (match exists with
        | `Yes -> Deferred.unit
        | `No | `Unknown ->
            Writer.write_line writer (String.concat ~sep:"," State_csv.csv_header);
            Deferred.unit) >>= fun () ->
        Writer.write_line writer (String.concat ~sep:"," (State_csv.row_of_t csv_row));
        Deferred.unit)

  let generate_session_id () =
    (* Generate a unique session ID using timestamp and random component *)
    let ts = Time_float_unix.now () |> Time_float_unix.to_span_since_epoch |> Time_ns.Span.of_span_float_round_nearest |> Time_ns.Span.to_int63_ns |> Int63.to_string in
    let rand = Random.bits () |> Int.to_string in
    sprintf "%s-%s" ts rand

  let create ?(name = "bot") ?session_id ?symbols ?order_ids ~api_nonce
      ?client_order_id_nonce ?state_csv_path () : t Deferred.t =
    let session_id =
      Option.value session_id
        ~default:(generate_session_id ())
    in
    Log.Global.info "Session.create: start name=%s session_id=%s" name session_id;
    ( match client_order_id_nonce with
    | None ->
        (* Include session_id in the nonce prefix for session-specific ordering *)
        let prefix = sprintf "%s-%s" name session_id in
        Client_order_id.pipe ~prefix ()
    | Some nonce -> nonce |> return )
    >>= fun client_order_id_nonce ->
    Log.Global.info "Session.create: got client_order_id_nonce";
    Events.create ?symbols ?order_ids (module C) api_nonce >>= fun events ->
    Log.Global.info "Session.create: Events.create finished";
    let t = { name; session_id; events; api_nonce; client_order_id_nonce } in
    (* Write initial state - include session_id in filename if not explicitly provided *)
    let state_csv_path = 
      match state_csv_path with
      | Some path -> path
      | None -> sprintf ".gemini_session_state_%s.csv" session_id
    in
    write_state_csv ~path:state_csv_path t >>| fun () ->
    Log.Global.info "Session.create: wrote initial state to %s" state_csv_path;
    t

  module New_order_request = struct
    type t =
      { symbol : Symbol.t;
        amount : string;
        price : string;
        side : Side.t;
        type_ : Order_type.t;
        options : Order_execution_option.t list
      }
    [@@deriving make, sexp]

    let make ?(type_ = `Exchange_limit) ?(options = [`Immediate_or_cancel]) = make_t ~type_ ~options

    let to_api ~client_order_id (t : t) : Order.New.request =
      let { symbol; amount; price; side; type_; options } = t in
      let api =
        Order.New.
          { client_order_id; symbol; amount; price; side; type_; options }
      in
      api
  end

  let submit_order ?(autoformat=`All) t (req : New_order_request.t) :
      [ `Ok of Order.New.response * status_pipe | Error.post ] Deferred.t =
    let req = match autoformat with
    | `All ->
        let formatted_price = format_price t req.symbol (Float.of_string req.price) in
        let formatted_quantity = format_quantity t req.symbol (Float.of_string req.amount) in
        {req with price = formatted_price; amount = formatted_quantity}      
    | `Price ->
        let formatted_price = format_price t req.symbol (Float.of_string req.price) in
        {req with price = formatted_price}
    | `Quantity -> 
        let formatted_quantity = format_quantity t req.symbol (Float.of_string req.amount) in
        {req with amount = formatted_quantity}
    | `None -> req in
    Inf_pipe.read t.client_order_id_nonce >>= fun client_order_id ->
    let req = New_order_request.to_api ~client_order_id req in
    Order.New.post (cfg t) t.api_nonce req >>| function
    | `Ok response ->
      let order_id = response.order_id in
      let status_pipe : _ Inf_pipe.Reader.t =
        status_pipe (cfg t) t.api_nonce order_id
      in
      let status_pipe, status_pipe' =
        Inf_pipe.fork ~pushback_uses:`Fast_consumer_only status_pipe
      in
      Concurrent_order_map.set ~key:order_id ~data:status_pipe
        t.events.order_status;
      `Ok (response, status_pipe')
    | #Error.post as e -> e

  let cancel_order t req =
    Order.Cancel.By_order_id.post (cfg t) t.api_nonce req >>| function
    | `Ok response ->
      Concurrent_order_map.remove t.events.order_status response.order_id;
      `Ok response
    | #Error.post as e -> e

  let cancel_all (t : t) =
    Order.Cancel.All.post (cfg t) t.api_nonce () >>| function
    | `Ok response ->
      Concurrent_order_map.clear t.events.order_status;
      `Ok response
    | #Error.post as e -> e
end

module Prod_session () = Make (Cfg.Production ())
module Sandbox_session () = Make (Cfg.Sandbox ())
