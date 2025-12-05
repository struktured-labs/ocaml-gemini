(** Websocket api support for the gemini trading exchange. *)
module type EVENT_TYPE = Csv_support.EVENT_TYPE
module type CSV_OF_EVENTS = Csv_support.CSV_OF_EVENTS
(** Specification for a websocket channel. *)
module type CHANNEL = sig
  (** The name of the channel *)
  val name : string

  (** The channel protocol version *)
  val version : string

  (** The uri path for this channel *)
  val path : string list

  (** Authentication syle for this channel. One of [`Private] or [`Public] *)
  val authentication : [ `Private | `Public ]

  (** Uri arguments which are appended to the end of the path segment *)
  type uri_args [@@deriving sexp, enumerate]

  (** Encder from well typed uri arguments to a string suitable for a uri. *)
  val encode_uri_args : uri_args -> string

  (** Defaut uri arguments. Optional for some channels. *)
  val default_uri_args : uri_args option

  (** Respone type of the channel. Must have sexp converters and a yojson parser *)
  type response [@@deriving sexp, of_yojson]

  module Event_type : EVENT_TYPE

  module Csv_of_event : CSV_OF_EVENTS with module Event_type = Event_type

  (** Given a response value produce csvable events modularized by event type. *)
  val events_of_response : response -> Csv_of_event.t

  (** Query parameters for the channel *)
  type query [@@deriving sexp]

  (** Encodes queries as an http header key value pair *)
  val encode_query : query -> string * string
end

module Error = struct
  type t =
    [ `Json_parse_error of string
    | `Channel_parse_error of string
    ]
  [@@deriving sexp, yojson]
end

module type RESULT = sig
  type response [@@deriving sexp, of_yojson]

  type ok = [ `Ok of response ] [@@deriving sexp, of_yojson]

  type t =
    [ ok
    | Error.t
    ]
  [@@deriving sexp, of_yojson]
end

module type CHANNEL_CLIENT_BASE = sig
  include CHANNEL

  module Error : module type of Error

  val command : string * Command.t
end

module type CHANNEL_CLIENT_INTERNAL = sig
  include CHANNEL_CLIENT_BASE

  val client :
    (module Cfg.S) ->
    ?query:Sexp.t list ->
    ?uri_args:uri_args ->
    ?nonce:int Inf_pipe.Reader.t ->
    unit ->
    [ `Ok of response | Error.t ] Pipe.Reader.t Deferred.t
end

module type CHANNEL_CLIENT_NO_REQUEST = sig
  include CHANNEL_CLIENT_BASE

  val client :
    (module Cfg.S) ->
    ?query:Sexp.t list ->
    ?uri_args:uri_args ->
    unit ->
    [ `Ok of response | Error.t ] Pipe.Reader.t Deferred.t
end
 
module type CHANNEL_CLIENT = sig
  include CHANNEL_CLIENT_BASE

  module Error : module type of Error

  val command : string * Command.t

  val client :
    (module Cfg.S) ->
    nonce:Nonce.reader ->
    ?query:Sexp.t list ->
    ?uri_args:uri_args ->
    unit ->
    [ `Ok of response | Error.t ] Pipe.Reader.t Deferred.t
end

(** Creates a websocket implementation given a [Channel] *)
module Impl (Channel : CHANNEL) :
  CHANNEL_CLIENT_INTERNAL
    with type response := Channel.response
     and type uri_args = Channel.uri_args
     and module Event_type = Channel.Event_type
     and type query = Channel.query
     and module Error = Error = struct
  (** Establishes a web socket client given configuration [Cfg] and optional
      [query], [uri_args] and [nonce] parameters.

      Produces a pipe of [(Channel.response, string) result] instances. *)

  include Channel
  module Error = Error

  let client (module Cfg : Cfg.S) ?query ?uri_args ?nonce () :
      [ `Ok of response | Error.t ] Pipe.Reader.t Deferred.t =
    let query =
      Option.map query ~f:(fun l ->
          List.fold ~init:String.Map.empty l ~f:(fun map q ->
              let key, data = Channel.encode_query (Channel.query_of_sexp q) in
              Map.add_multi map ~key ~data )
          |> fun map ->
          let keys = Map.keys map in
          List.map keys ~f:(fun k -> (k, Map.find_multi map k)) )
    in
    let uri =
      Uri.make ~host:Cfg.api_host ~scheme:"wss" ?query
        ~path:
          (String.concat ~sep:"/"
             ( Channel.path
             @ Option.(map ~f:Channel.encode_uri_args uri_args |> to_list) ) )
        ()
    in
    Log.Global.info "Ws.client: uri=%s" (Uri.to_string uri);
    let payload = `Null in
    let path = Path.to_string Channel.path in
    let%bind payload =
      match nonce with
      | None -> return None
      | Some nonce ->
        Nonce.Request.(make ~nonce ~request:path ~payload () >>| to_yojson)
        >>| fun s -> Yojson.Safe.to_string s |> Option.some
    in
    let headers =
      ( match Channel.authentication with
      | `Private ->
        Option.map
          ~f:(fun p -> Auth.(to_headers (module Cfg) (of_payload p)))
          payload
      | `Public -> None )
      |> Option.value ~default:(Cohttp.Header.init ())
    in
    Cohttp_async_websocket.Client.create ~headers uri >>= fun x ->
    Or_error.ok_exn x |> return >>= fun (_response, ws) ->
    let r, _w = Websocket.pipes ws in
    Log.Global.flushed () >>| fun () ->
    let pipe =
      Pipe.map r ~f:(fun s ->
          Log.Global.debug "json of event: %s" s;
          ( try `Ok (Yojson.Safe.from_string s) with
          | Yojson.Json_error e -> `Json_parse_error e )
          |> function
          | #Error.t as e -> e
          | `Ok json -> (
            match Channel.response_of_yojson json with
            | Ok response -> `Ok response
            | Error e -> `Channel_parse_error e ) )
    in
    pipe

  let command =
    let spec : (_, _) Command.Spec.t =
      let open Command.Spec in
      empty +> Cfg.param
      +> flag "-loglevel" (optional int) ~doc:"1-3 loglevel"
      +> flag "-query" (listed sexp) ~doc:"QUERY query parameters"
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
    let run cfg loglevel query (csv_dir : string option) no_csv uri_args () =
      let cfg = Cfg.or_default cfg in
      let module Cfg = (val cfg : Cfg.S) in
      Option.iter loglevel ~f:set_loglevel;
      let uri_args =
        Option.first_some
          (Option.map ~f:Channel.uri_args_of_sexp uri_args)
          Channel.default_uri_args
      in
      let query =
        match List.is_empty query with
        | true -> None
        | false -> Some query
      in
      let%bind nonce = Nonce.File.(pipe ~init:default_filename) () in
      Log.Global.info "Initiating channel %s with path %s" Channel.name
        (Path.to_string Channel.path);
      let channel_to_sexp_str response =
        Channel.sexp_of_response response
        |> Sexp.to_string_hum |> sprintf "%s\n"
      in
      let append_to_csv =
        if no_csv then
          fun _ ->
        ()
        else
          fun response ->
        let events = Channel.events_of_response response in
        let all = Channel.Csv_of_event.write_all ?dir:csv_dir events in
        let tags =
          List.map all ~f:(fun (k, v) ->
              (Channel.Event_type.to_string k, Int.to_string v) )
        in
        Log.Global.debug ~tags "wrote csv response events";
        ()
      in
      let ok_pipe_reader response =
        append_to_csv response;
        channel_to_sexp_str response
      in
      client (module Cfg) ?query ?uri_args ~nonce () >>= fun pipe ->
      Log.Global.debug "Broadcasting channel %s to stderr..." Channel.name;
      let pipe =
        Pipe.filter_map pipe ~f:(function
          | `Ok ok -> Some ok
          | #Error.t as e ->
            Log.Global.error "Failed to parse last event: %s"
              (Error.sexp_of_t e |> Sexp.to_string);
            None )
      in
      Pipe.transfer pipe Writer.(pipe (Lazy.force stderr)) ~f:ok_pipe_reader
    in
    ( Channel.name,
      Command.async_spec
        ~summary:
          (sprintf "Gemini %s %s Websocket Command" Channel.version Channel.name)
        spec run )
end

(** Create a websocket interface that has no request parameters *)
module Make_no_request (Channel : CHANNEL with type query = unit) :
  CHANNEL_CLIENT_NO_REQUEST
    with type response := Channel.response
     and type uri_args := Channel.uri_args
     and module Event_type := Channel.Event_type
     and type query := unit = struct
  include Channel
  include Impl (Channel)

  let client = client ?nonce:None
end

(** Create a websocket interface with request parameters *)
module Make (Channel : CHANNEL) :
  CHANNEL_CLIENT
    with type response := Channel.response
     and type uri_args := Channel.uri_args
     and module Event_type := Channel.Event_type
     and type query := Channel.query = struct
  include Channel
  include Impl (Channel)

  let client (module Cfg : Cfg.S) ~nonce = client (module Cfg) ~nonce
end
