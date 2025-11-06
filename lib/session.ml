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
        Symbol.Map.t
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
    Deferred.List.map symbols ~how:`Sequential ~f:(fun symbol ->
        Log.Global.info "Events.create: Market_data.client %s"
          (Symbol.to_string symbol);
        Market_data.client cfg ?query:None ~uri_args:symbol () >>= fun pipe ->
        Log.Global.debug "Events.create: Market_data.client ready for %s"
          (Symbol.to_string symbol);
        let pipe =
          Pipe.filter_map pipe ~f:(function
            | `Ok { Market_data.message = `Update { events; _ }; _ } -> Some events
            | `Ok { message = `Heartbeat _; _ } -> None
            | #Market_data.Error.t as e ->
              Log.Global.error_s (Market_data.Error.sexp_of_t e);
              None )
        in
        Log.Global.debug "Events.create: creating relay pipe for %s"
          (Symbol.to_string symbol);
        let reader, writer = Pipe.create () in
        don't_wait_for
          ( Pipe.iter pipe ~f:(fun events ->
                Log.Global.debug "Events.create: relaying %d events for %s"
                  (Array.length events)
                  (Symbol.to_string symbol);
                Pipe.transfer_in writer ~from:(Queue.of_array events) )
            >>| fun () ->
            Log.Global.info "Events.create: upstream closed for %s, closing writer"
              (Symbol.to_string symbol);
            Pipe.close writer );
        Log.Global.debug "Events.create: returning Inf_pipe reader for %s"
          (Symbol.to_string symbol);
        return (Inf_pipe.Reader.create reader) )
    >>= fun market_data ->
    Log.Global.debug "Events.create: market_data readers created";
    let market_data =
      Symbol.Map.of_alist_exn (List.zip_exn symbols market_data)
    in
    Log.Global.info "Events.create: starting order_books.pipe";
    Order_book.Books.pipe cfg ~symbols () >>= fun order_books ->
    Log.Global.info "Events.create: order_books.pipe ready";
    Log.Global.info "Events.create: starting order_events.client";
    Order_events.client cfg ~nonce ?query:None ?uri_args:None ()
    >>= fun order_events ->
    Log.Global.info "Events.create: order_events.client ready";
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
      Map.map ~f:(Pipe.map ~f:Poly_ok.ok_exn') order_books_exn
    in
    let order_events_exn = Pipe.map ~f:Poly_ok.ok_exn' order_events_exn in
    Log.Global.info "Events.create: starting Ledger.pipe";
    Ledger.pipe ?num_values:None ?how:None ?behavior:None ~init order_books_exn
      order_events_exn
    >>| fun ledger ->
    Log.Global.info "Events.create: finished Ledger.pipe, assembling record";
    { balance;
      trades;
      order_status;
      order_events;
      market_data;
      order_books;
      ledger
    }

  let balance (t : t) = t.balance

  let trade (t : t) = t.trades

  let market_data (t : t) = t.market_data

  let order_events (t : t) = t.order_events
end

module Make (C : Cfg.S) = struct
  type t =
    { name : string;
      api_nonce : Nonce.reader;
      client_order_id_nonce : Client_order_id.reader;
      events : Events.t
    }

  let name t = t.name

  let api_nonce t = t.api_nonce

  let cfg (_t : t) = (module C : Cfg.S)

  let create ?(name = "bot") ?symbols ?order_ids ~api_nonce
      ?client_order_id_nonce () : t Deferred.t =
    Log.Global.info "Session.create: start name=%s" name;
    ( match client_order_id_nonce with
    | None -> Client_order_id.pipe ~prefix:name ()
    | Some nonce -> nonce |> return )
    >>= fun client_order_id_nonce ->
    Log.Global.info "Session.create: got client_order_id_nonce";
    Events.create ?symbols ?order_ids (module C) api_nonce >>| fun events ->
    Log.Global.info "Session.create: Events.create finished";
    { name; events; api_nonce; client_order_id_nonce }

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

    let make ?(type_ = `Exchange_limit) ?(options = []) = make_t ~type_ ~options

    let to_api ~client_order_id (t : t) : Order.New.request =
      let { symbol; amount; price; side; type_; options } = t in
      let api =
        Order.New.
          { client_order_id; symbol; amount; price; side; type_; options }
      in
      api
  end

  let submit_order t (req : New_order_request.t) :
      [ `Ok of Order.New.response * status_pipe | Error.post ] Deferred.t =
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
