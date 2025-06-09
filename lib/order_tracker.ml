open Common
module Order_event = V1.Order_events.Order_event
module Status = V1.Order.Status

module T = struct
  module Order_map = Map.Make (Int_number)

  module Order = struct
    module Update_source = struct
      module T = struct
        type t =
          [ `Order_response
          | `Order_event
          ]
        [@@deriving sexp, compare, equal, enumerate]

        let to_string = function
          | `Order_response -> "order_response"
          | `Order_event -> "order_event"
      end

      include T

      include (Json.Make (T) : Json.S with type t := t)
    end

    type t =
      { order_id : int64;
        symbol : Symbol.Enum_or_string.t;
        side : Side.t;
        price : Price.Option.t;
        original_amount : Decimal_number.Option.t;
        executed_amount : Decimal_number.Option.t;
        remaining_amount : Decimal_number.Option.t;
        is_cancelled : bool;
        is_live : bool;
        client_order_id : Client_order_id.Option.t;
        timestamp : Timestamp.t;
        update_source : Update_source.t
      }
    [@@deriving sexp, fields, compare, equal, csv]

    let of_order_response (response : Status.response) =
      let number_of_str_some x = Decimal_number.of_string x |> Option.some in
      { order_id = response.order_id;
        symbol = response.symbol;
        side = response.side;
        price = number_of_str_some response.price;
        original_amount = number_of_str_some response.original_amount;
        executed_amount = number_of_str_some response.executed_amount;
        remaining_amount = number_of_str_some response.remaining_amount;
        is_cancelled = response.is_cancelled;
        is_live = response.is_live;
        client_order_id = response.client_order_id;
        timestamp = response.timestamp;
        update_source = `Order_response
      }

    let of_order_event (event : Order_event.t) =
      let number_of_str_opt = Option.map ~f:Decimal_number.of_string in
      { order_id = Int64.of_string event.order_id;
        symbol = event.symbol;
        side = event.side;
        price = number_of_str_opt event.price;
        original_amount = number_of_str_opt event.original_amount;
        executed_amount = number_of_str_opt event.executed_amount;
        remaining_amount = number_of_str_opt event.remaining_amount;
        is_cancelled = event.is_cancelled;
        is_live = event.is_live;
        client_order_id = event.client_order_id;
        timestamp = event.timestamp;
        update_source = `Order_event
      }
  end

  type summary =
    { total_original : float;
      total_executed : float;
      total_remaining : float
    }
  [@@deriving sexp, fields, compare, equal, hash]

  let empty_summary =
    { total_original = 0.; total_executed = 0.; total_remaining = 0. }

  type t =
    { orders : Order.t Order_map.t;
      summary : summary
    }
  [@@deriving sexp]

  let order (t : t) order_id = Map.find t.orders order_id

  let empty : t = { orders = Order_map.empty; summary = empty_summary }

  let summary_of_orders orders =
    Map.fold
      ~f:(fun
          ~key:_
          ~(data : Order.t)
          { total_original; total_executed; total_remaining }
        ->
        let event = data in
        let original_amount = Option.value event.original_amount ~default:0. in
        let executed_amount = Option.value event.executed_amount ~default:0. in
        let remaining_amount =
          Option.value event.remaining_amount ~default:0.
        in
        { total_original = total_original +. original_amount;
          total_executed = total_executed +. executed_amount;
          total_remaining = total_remaining +. remaining_amount
        } )
      orders ~init:empty_summary

  let summary t = t.summary

  let total_executed t =
    summary t |> fun { total_executed; _ } -> total_executed

  let total_remaining t =
    summary t |> fun { total_remaining; _ } -> total_remaining

  let total_original t =
    summary t |> fun { total_original; _ } -> total_original

  let on_order_event (t : t) (event : Order_events.Order_event.t) =
    let order_id = event.order_id |> Int_number.of_string in
    let orders =
      match
        event.is_cancelled || (not event.is_live)
        || Option.value_map
             ~f:(fun r -> Float.equal 0. (Float.of_string r))
             event.remaining_amount ~default:false
      with
      | true -> Map.remove t.orders order_id
      | false ->
        Map.update t.orders order_id ~f:(function _ ->
            Order.of_order_event event )
    in
    let summary = summary_of_orders t.orders in
    { orders; summary }

  let on_order_response (t : t) (response : Status.response) =
    let order_id = response.order_id in
    let orders =
      match
        response.is_cancelled || (not response.is_live)
        || Float.equal 0. (Float.of_string response.remaining_amount)
      with
      | true -> Map.remove t.orders order_id
      | false ->
        Map.update t.orders order_id ~f:(function _ ->
            Order.of_order_response response )
    in
    let summary = summary_of_orders t.orders in
    { orders; summary }
end

include T
