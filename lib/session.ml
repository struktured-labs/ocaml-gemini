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

  let default_filename = "~/.marbl/client_order_id"

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
    let balance = balance_pipe cfg nonce in
    let trades =
      List.map symbols ~f:(fun symbol -> (symbol, trade_pipe cfg nonce symbol))
      |> Symbol.Map.of_alist_exn
    in
    let order_status =
      List.map order_ids ~f:(fun order_id ->
          (order_id, status_pipe cfg nonce order_id) )
      |> Order_map.of_alist_exn |> Concurrent_order_map.of_map
    in
    Deferred.List.map symbols ~how:`Sequential ~f:(fun symbol ->
        Market_data.client cfg ?query:None ~uri_args:symbol () >>= fun pipe ->
        Pipe.filter_map pipe ~f:(function
          | `Ok { Market_data.message = `Update { events; _ }; _ } ->
            Some events
          | `Ok { message = `Heartbeat _; _ } -> None
          | #Market_data.Error.t as e -> begin
            Log.Global.error_s (Market_data.Error.sexp_of_t e);
            None
          end )
        |> return
        >>= fun pipe ->
        let reader, writer = Pipe.create () in
        Pipe.iter pipe ~f:(fun events ->
            Pipe.transfer_in writer ~from:(Queue.of_array events) )
        >>| fun () -> Inf_pipe.Reader.create reader )
    >>= fun market_data ->
    let market_data =
      Symbol.Map.of_alist_exn (List.zip_exn symbols market_data)
    in
    Order_book.Books.pipe cfg ~symbols () >>= fun order_books ->
    Order_events.client cfg ~nonce ?query:None ?uri_args:None ()
    >>= fun order_events ->
    let init = Symbol.Map.empty in
    let order_events, order_events_exn =
      Pipe.fork ~pushback_uses:`Fast_consumer_only order_events
    in
    let order_books_fork =
      Map.map ~f:(Pipe.fork ~pushback_uses:`Fast_consumer_only) order_books
    in
    let order_books, order_books_exn =
      (Map.map order_books_fork ~f:fst, Map.map order_books_fork ~f:snd)
    in
    let order_books_exn =
      Map.map ~f:(Pipe.map ~f:Poly_ok.ok_exn') order_books_exn
    in
    let order_events_exn = Pipe.map ~f:Poly_ok.ok_exn' order_events_exn in
    Ledger.pipe ?num_values:None ?how:None ?behavior:None ~init order_books_exn
      order_events_exn
    >>| fun ledger ->
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
    ( match client_order_id_nonce with
    | None -> Client_order_id.pipe ~prefix:name ()
    | Some nonce -> nonce |> return )
    >>= fun client_order_id_nonce ->
    Events.create ?symbols ?order_ids (module C) api_nonce >>| fun events ->
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
