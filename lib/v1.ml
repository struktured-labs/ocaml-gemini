open Common
module Auth = Auth
module Cfg = Cfg
module Nonce = Nonce
module Rest = Rest
module Result = Json.Result
module Inf_pipe = Inf_pipe
module Poly_ok = Poly_ok

module T = struct
  let path = [ "v1" ]

  module Side = Side
  module Exchange = Exchange
  module Timestamp = Timestamp
  module Market_data = Market_data
  module Order_events = Order_events
  module Currency = Currency
  module Symbol = Symbol
  module Order_type = Order_type

  module Heartbeat = struct
    module T = struct
      let name = "heartbeat"

      let path = path @ [ "heartbeat" ]

      type uri_args = unit
      let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
      let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
      let all_of_uri_args = [ () ]
      let encode_uri_args (_:uri_args) = ""
      let default_uri_args : uri_args option = None

      type request = unit [@@deriving sexp, yojson]

      type response = { result : bool [@default true] }
      [@@deriving sexp, of_yojson]
    end

    include T
    include Rest.Make_no_arg (T)
  end

  module Order_execution_option = struct
    module T = struct
      type t =
        [ `Maker_or_cancel
        | `Immediate_or_cancel
        | `Auction_only
        ]
      [@@deriving sexp, enumerate, compare, equal]

      let to_string = function
        | `Maker_or_cancel -> "maker_or_cancel"
        | `Immediate_or_cancel -> "immediate_or_cancel"
        | `Auction_only -> "auction_only"
    end

    include T

    include (Json.Make (T) : Json.S with type t := t)
  end

  module Order = struct
    let name = "order"

    let path = path @ [ "order" ]

    module Status = struct
      module T = struct
        let name = "status"

        let path = path @ [ "status" ]

        type uri_args = unit
        let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
        let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
        let all_of_uri_args = [ () ]
        let encode_uri_args (_:uri_args) = ""
        let default_uri_args : uri_args option = None

        type request = { order_id : Int_number.t } [@@deriving yojson, sexp]

        type response =
          { client_order_id : Client_order_id.t option; [@default None]
            order_id : Int_string.t;
            id : Int_string.t;
            symbol : Symbol.Enum_or_string.t;
            exchange : Exchange.t;
            avg_execution_price : Decimal_string.t;
            side : Side.t;
            type_ : Order_type.t; [@key "type"]
            timestamp : Timestamp.Sec.t;
            timestampms : Timestamp.Ms.t;
            is_live : bool;
            is_cancelled : bool;
            is_hidden : bool;
            was_forced : bool;
            executed_amount : Decimal_string.t;
            remaining_amount : Decimal_string.t;
            options : Order_execution_option.t list;
            price : Decimal_string.t;
            original_amount : Decimal_string.t
          }
        [@@deriving yojson, sexp]
      end

      include T
      include Rest.Make (T)
    end

    module New = struct
      module T = struct
        let name = "new"

        let path = path @ [ "new" ]

        type uri_args = unit
        let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
        let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
        let all_of_uri_args = [ () ]
        let encode_uri_args (_:uri_args) = ""
        let default_uri_args : uri_args option = None

        type request =
          { client_order_id : Client_order_id.t;
            symbol : Symbol.t;
            amount : Decimal_string.t;
            price : Decimal_string.t;
            side : Side.t;
            type_ : Order_type.t; [@key "type"]
            options : Order_execution_option.t list
          }
        [@@deriving sexp, yojson]

        type response = Status.response [@@deriving of_yojson, sexp]
      end

      include T
      include Rest.Make (T)
    end

    module Cancel = struct
      let name = "cancel"

      let path = path @ [ "cancel" ]

      module By_order_id = struct
        module T = struct
          let name = "by-order-id"

          let path = path

          type uri_args = unit
          let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
          let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
          let all_of_uri_args = [ () ]
          let encode_uri_args (_:uri_args) = ""
          let default_uri_args : uri_args option = None

          type request = { order_id : Int_string.t } [@@deriving sexp, yojson]

          type response = Status.response [@@deriving sexp, of_yojson]
        end

        include T
        include Rest.Make (T)
      end

      type details =
        { cancelled_orders : Status.response list; [@key "cancelledOrders"]
          cancel_rejects : Status.response list [@key "cancelRejects"]
        }
      [@@deriving sexp, yojson]

      module All = struct
        module T = struct
          let name = "all"

          let path = path @ [ "all" ]

          type uri_args = unit
          let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
          let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
          let all_of_uri_args = [ () ]
          let encode_uri_args (_:uri_args) = ""
          let default_uri_args : uri_args option = None

          type request = unit [@@deriving sexp, yojson]

          type response = { details : details } [@@deriving sexp, of_yojson]
        end

        include T
        include Rest.Make_no_arg (T)
      end

      module Session = struct
        module T = struct
          let name = "session"

          let path = path @ [ "session" ]

          type uri_args = unit
          let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
          let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
          let all_of_uri_args = [ () ]
          let encode_uri_args (_:uri_args) = ""
          let default_uri_args : uri_args option = None

          type request = unit [@@deriving sexp, yojson]

          type response = { details : details } [@@deriving sexp, of_yojson]
        end

        include T
        include Rest.Make_no_arg (T)
      end

      let command : string * Command.t =
        ( name,
          Command.group
            ~summary:(Path.to_summary ~has_subnames:true path)
            [ By_order_id.command; Session.command; All.command ] )
    end

    let command : string * Command.t =
      ( name,
        Command.group
          ~summary:(Path.to_summary ~has_subnames:true path)
          [ New.command; Cancel.command; Status.command ] )
  end

  module Orders = struct
    module T = struct
      let name = "orders"

      let path = path @ [ "orders" ]

      type uri_args = unit
      let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
      let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
      let all_of_uri_args = [ () ]
      let encode_uri_args (_:uri_args) = ""
      let default_uri_args : uri_args option = None

      type request = unit [@@deriving sexp, yojson]

      type response = Order.Status.response list [@@deriving of_yojson, sexp]
    end

    include T
    include Rest.Make_no_arg (T)
  end

  module Mytrades = struct
    type trade =
      { price : Decimal_string.t;
        amount : Decimal_string.t;
        timestamp : Timestamp.Sec.t;
        timestampms : Timestamp.Ms.t;
        type_ : Side.t; [@key "type"]
        aggressor : bool;
        fee_currency : Currency.Enum_or_string.t;
        fee_amount : Decimal_string.t;
        tid : Int_number.t;
        order_id : Int_string.t;
        client_order_id : Client_order_id.t option; [@default None]
        is_auction_fill : bool;
        is_clearing_fill : bool;
        symbol : Symbol.Enum_or_string.t;
        exchange : Exchange.t
      }
    [@@deriving yojson, sexp]

    module T = struct
      let name = "mytrades"

      let path = path @ [ "mytrades" ]

      type uri_args = unit
      let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
      let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
      let all_of_uri_args = [ () ]
      let encode_uri_args (_:uri_args) = ""
      let default_uri_args : uri_args option = None

      type request =
        { symbol : Symbol.t;
          limit_trades : int option; [@default None]
          timestamp : Timestamp.Sec.t option [@default None]
        }
      [@@deriving sexp, yojson]

      type response = trade list [@@deriving of_yojson, sexp]
    end

    include T
    include Rest.Make (T)
  end

  module Tradevolume = struct
    type volume =
      { account_id : (Int_number.t option[@default None]);
        symbol : Symbol.Enum_or_string.t;
        base_currency : Currency.Enum_or_string.t;
        notional_currency : Currency.Enum_or_string.t;
        data_date : string;
            (*TODO use timestamp or a date module with MMMM-DD-YY *)
        total_volume_base : Decimal_number.t;
        maker_buy_sell_ratio : Decimal_number.t;
        buy_maker_base : Decimal_number.t;
        buy_maker_notional : Decimal_number.t;
        buy_maker_count : Int_number.t;
        sell_maker_base : Decimal_number.t;
        sell_maker_notional : Decimal_number.t;
        sell_maker_count : Int_number.t;
        buy_taker_base : Decimal_number.t;
        buy_taker_notional : Decimal_number.t;
        buy_taker_count : Int_number.t;
        sell_taker_base : Decimal_number.t;
        sell_taker_notional : Decimal_number.t;
        sell_taker_count : Int_number.t
      }
    [@@deriving yojson, sexp, fields]

    module T = struct
      let name = "tradevolume"

      let path = path @ [ "tradevolume" ]

      type uri_args = unit
      let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
      let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
      let all_of_uri_args = [ () ]
      let encode_uri_args (_:uri_args) = ""
      let default_uri_args : uri_args option = None

      type request = unit [@@deriving yojson, sexp]

      type response = volume list [@@deriving sexp]

      type nested_response = volume list list list [@@deriving sexp, of_yojson]

      let response_of_yojson (yojson : Yojson.Safe.t) :
          (response, 'err) Result.t =
        nested_response_of_yojson yojson
        |> Result.map ~f:(fun events ->
               List.bind events ~f:Fn.id |> List.bind ~f:Fn.id )
    end

    include T
    include Rest.Make_no_arg (T)
  end

  module Balances = struct
    module T = struct
      let name = "balances"

      let path = path @ [ "balances" ]

      type uri_args = unit
      let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
      let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
      let all_of_uri_args = [ () ]
      let encode_uri_args (_:uri_args) = ""
      let default_uri_args : uri_args option = None

      type request = unit [@@deriving yojson, sexp]

      type balance =
        { currency : Currency.Enum_or_string.t;
          amount : Decimal_string.t;
          available : Decimal_string.t;
          available_for_withdrawal : Decimal_string.t;
              [@key "availableForWithdrawal"]
          type_ : string [@key "type"]
        }
      [@@deriving yojson, sexp]

      type response = balance list [@@deriving of_yojson, sexp]
    end

    include T
    include Rest.Make_no_arg (T)
  end

  module Notional_volume = struct
    module T = struct
      let name = "notionalvolume"

      let path = path @ [ "notionalvolume" ]

      type uri_args = unit
      let sexp_of_uri_args = Sexplib0.Sexp_conv.sexp_of_unit
      let uri_args_of_sexp = Sexplib0.Sexp_conv.unit_of_sexp
      let all_of_uri_args = [ () ]
      let encode_uri_args (_:uri_args) = ""
      let default_uri_args : uri_args option = None

      type request =
        { symbol : Symbol.t option; [@default None]
          account : string option [@default None]
        }
      [@@deriving yojson, sexp, fields]

      type notional_1d_volume =
        { date : string; (* TODO use strict date type *)
          notional_volume : Decimal_number.t
        }
      [@@deriving sexp, yojson]

      type response =
        { last_updated_ms : Timestamp.Ms.t;
          web_maker_fee_bps : Int_number.t;
          web_taker_fee_bps : Int_number.t;
          web_auction_fee_bps : Int_number.t option; [@default None]
          api_maker_fee_bps : Int_number.t;
          api_taker_fee_bps : Int_number.t;
          api_auction_fee_bps : Int_number.t option; [@default None]
          fix_maker_fee_bps : Int_number.t;
          fix_taker_fee_bps : Int_number.t;
          fix_auction_fee_bps : Int_number.t option; [@default None]
          block_maker_fee_bps : Int_number.t;
          block_taker_fee_bps : Int_number.t;
          date : string; (* TODO use strict date type *)
          notional_30d_volume : Decimal_number.t;
          notional_1d_volume : notional_1d_volume list
        }
      [@@deriving yojson, sexp]
    end

    include T
    include Rest.Make (T)
  end

  module Symbol_details = struct
    (* https://docs.gemini.com/rest/market-data#get-symbol-details *)
    module T = struct
      let name = "symboldetails"

      let path = path @ [ "symbols"; "details" ]

      type uri_args = Symbol.t
      let sexp_of_uri_args = Symbol.sexp_of_t
      let uri_args_of_sexp = Symbol.t_of_sexp
      let all_of_uri_args = Symbol.all
      let encode_uri_args (s:uri_args) = Symbol.to_string s
      let default_uri_args : uri_args option = None

      type request = { symbol : Symbol.t } [@@deriving yojson, sexp]

      type response =
        { symbol : Symbol.Enum_or_string.t;
          base_currency : Currency.Enum_or_string.t;
          quote_currency : Currency.Enum_or_string.t;
          tick_size : Decimal_number.t;
          quote_increment : Decimal_number.t;
          min_order_size : Decimal_string.t;
          status : string;
          wrap_enabled : bool;
          product_type : string;
          contract_type : string;
          contract_price_currency : Currency.Enum_or_string.t
        }
      [@@deriving yojson, sexp]
    end

    include T
    module Impl = Rest.Make (T)

    let post (module Cfg : Cfg.S) ?uri_args _nonce (req : T.request) =
      let symbol = Option.value uri_args ~default:req.symbol in
      let segments = path @ [ Symbol.to_string symbol ] in
      let path_str = Path.to_string segments in
      let uri = Uri.make ~scheme:"https" ~host:Cfg.api_host ~path:path_str () in
      Cohttp_async.Client.get uri >>= fun (response, body) ->
      match Cohttp.Response.status response with
      | `OK ->
        Cohttp_async.Body.to_string body >>| fun s ->
        let yojson = Yojson.Safe.from_string s in
        (match T.response_of_yojson yojson with
         | Result.Ok x -> `Ok x
         | Result.Error e -> `Json_parse_error Rest.Error.{ message = e; body = s })
      | `Not_found -> return `Not_found
      | `Not_acceptable -> Cohttp_async.Body.to_string body >>| fun b -> `Not_acceptable b
      | `Bad_request -> Cohttp_async.Body.to_string body >>| fun b -> `Bad_request b
      | `Service_unavailable -> Cohttp_async.Body.to_string body >>| fun b -> `Service_unavailable b
      | `Unauthorized -> Cohttp_async.Body.to_string body >>| fun b -> `Unauthorized b
      | (code : Cohttp.Code.status_code) ->
        Cohttp_async.Body.to_string body >>| fun b ->
        failwiths ~here:[%here]
          (sprintf "unexpected status code (body=%S)" b)
          code Cohttp.Code.sexp_of_status_code

    let command : string * Command.t =
      let open Command.Let_syntax in
      ( name,
        Command.async
          ~summary:(Path.to_summary ~has_subnames:false path)
          [%map_open
            let config = Cfg.param
            and request = anon ("request" %: sexp) in
            fun () ->
              let request = T.request_of_sexp request in
              Log.Global.info "request:\n %s"
                (T.sexp_of_request request |> Sexp.to_string);
              let config = Cfg.or_default config in
              Nonce.File.(pipe ~init:default_filename) () >>= fun nonce ->
              post config ~uri_args:request.symbol nonce request >>= function
              | `Ok response ->
                Log.Global.info "response:\n %s"
                  (Sexp.to_string_hum (T.sexp_of_response response));
                Log.Global.flushed ()
              | #Rest.Error.post as post_error ->
                failwiths ~here:[%here]
                  (sprintf "post for operation %S failed"
                     (Path.to_string path) )
                  post_error Rest.Error.sexp_of_post] )
  end
end

include T
