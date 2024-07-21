(** Csv support, including generic optional and list types. *)
module Core_list = List

module Optional = struct
  module type ARGS = sig
    type t [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Stringable with type t := t

    val null : string
  end

  module Default_args (Args : sig
    type t [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Stringable with type t := t
  end) : ARGS with type t = Args.t = struct
    let null = ""

    include Args
  end

  module type S = sig
    type elt [@@deriving sexp, yojson, compare, equal]

    type t = elt option [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Csvable with type t := t
  end

  module Make (C : ARGS) : S with type elt = C.t = struct
    type elt = C.t [@@deriving sexp, yojson, compare, equal]

    module T = struct
      type t = elt option [@@deriving sexp, yojson, compare, equal]

      let to_string t = Option.value_map ~default:C.null t ~f:C.to_string

      let of_string s =
        if String.equal C.null s then
          None
        else
          Some (C.of_string s)
    end

    include T

    include (Csvfields.Csv.Atom (T) : Csvfields.Csv.Csvable with type t := t)
  end

  module Make_default (Args : sig
    type t [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Stringable with type t := t
  end) =
    Make (Default_args (Args))

  module String = Make_default (struct
    type t = string [@@deriving yojson, compare, equal]

    include (String : module type of String with type t := t)
  end)
end

module List = struct
  module type ARGS = sig
    type t [@@deriving sexp, yojson, compare, compare, equal]

    include Csvfields.Csv.Stringable with type t := t

    val sep : char
  end

  module Default_args (Args : sig
    type t [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Stringable with type t := t
  end) =
  struct
    let sep = ' '

    include Args
  end

  module type S = sig
    type elt [@@deriving sexp, yojson, compare, equal]

    type t = elt list [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Csvable with type t := t
  end

  module Make (C : ARGS) : S with type elt = C.t = struct
    type elt = C.t [@@deriving sexp, yojson, compare, equal]

    module T = struct
      type t = elt list [@@deriving sexp, yojson, compare, equal]

      let to_string t =
        Core_list.map ~f:C.to_string t |> String.concat ~sep:(Char.to_string C.sep)

      let of_string s = String.split ~on:C.sep s |> List.map ~f:C.of_string
    end

    include T

    include (Csvfields.Csv.Atom (T) : Csvfields.Csv.Csvable with type t := t)
  end

  module Make_default (Args : sig
    type t [@@deriving sexp, yojson, compare, equal]

    include Csvfields.Csv.Stringable with type t := t
  end) =
    Make (Default_args (Args))

  module String = Make (Default_args (struct
    type t = string [@@deriving yojson]

    include (String : module type of String with type t := t)
  end))
end

let write_header out header =
  let s = sprintf "%s\n" (String.concat ~sep:"," header) in
  Log.Global.debug "csv_support: writing header: %s" s;
  Out_channel.output_string out s

module type CSVABLE = Csvfields.Csv.Csvable

module type EVENT_CSVABLE = sig
  include CSVABLE

  val events : t list
end

module type EVENT_TYPE = sig
  type t [@@deriving sexp]

  include Json.S with type t := t

  include Comparable.S with type t := t

  val equal : t -> t -> bool

  val __t_of_sexp__ : Sexp.t -> t
end

module type CSV_OF_EVENTS = sig
  module Event_type : EVENT_TYPE

  type t

  val empty : t

  val add : t -> Event_type.t -> (module EVENT_CSVABLE) -> t

  val add' :
    t -> Event_type.t -> (module CSVABLE with type t = 'a) -> 'a list -> t

  val csv_header : t -> Event_type.t -> string list option

  val get : t -> Event_type.t -> string list list

  val all : t -> (Event_type.t * string list list) list

  val write : ?dir:string -> t -> Event_type.t -> int

  val write_all : ?dir:string -> t -> (Event_type.t * int) list
end

module type WRITER = sig
  module Csvable : CSVABLE

  type t

  val empty : t

  val of_list : Csvable.t list -> t

  val add : t -> Csvable.t -> t

  val add' : t -> Csvable.t list -> t

  val csv_header : t -> string list

  val get : t -> string list list

  val write : ?dir:string -> name:string -> t -> int
end

let maybe_write_header out filename header =
  Option.iter
    ( try
        let stat = Core_unix.stat filename in
        if Int64.(equal stat.st_size zero) then
          Some header
        else
          None
      with
    | Unix.Unix_error (Unix.Error.ENOENT, "stat", _f) -> Some header )
    ~f:(write_header out)

module Event_writer (Event_type : EVENT_TYPE) :
  CSV_OF_EVENTS with module Event_type = Event_type = struct
  type t = (module EVENT_CSVABLE) list Event_type.Map.t

  let empty : t = Event_type.Map.empty

  module Event_type = Event_type

  let add (t : t) (event_type : Event_type.t) (module Event : EVENT_CSVABLE) =
    Map.add_multi t ~key:event_type ~data:(module Event : EVENT_CSVABLE)

  let add' (t : t) (type event) (event_type : Event_type.t)
      (module Event : CSVABLE with type t = event) (events : event list) =
    let module E = struct
      include Event

      let events = events
    end in
    add t event_type (module E)

  let csv_header (t : t) event_type =
    Map.find_multi t event_type
    |> Core_list.hd
    |> Option.map ~f:(fun (module Event : EVENT_CSVABLE) -> Event.csv_header)

  let get (t : t) (event_type : Event_type.t) =
    Map.find_multi t event_type
    |> Core_list.concat_map ~f:(fun (module Event : EVENT_CSVABLE) ->
           Event.events |> Core_list.map ~f:Event.row_of_t )

  let write ?dir (t : t) (event_type : Event_type.t) =
    let dir =
      match dir with
      | None -> Core_unix.getcwd ()
      | Some dir -> dir
    in
    let filename =
      Filename.concat dir (sprintf "%s.csv" (Event_type.to_string event_type))
    in
    Out_channel.with_file ~append:true ~binary:false ~fail_if_exists:false
      filename ~f:(fun out ->
        Option.iter (csv_header t event_type)
          ~f:(maybe_write_header out filename);
        Map.find_multi t event_type
        |> Core_list.fold ~init:0 ~f:(fun acc (module Event : EVENT_CSVABLE) ->
               let events = Event.events in
               let len = Core_list.length events in
               Event.csv_save_out out events;
               acc + len ) )

  let all (t : t) =
    Core_list.filter_map Event_type.all ~f:(fun e ->
        match get t e with
        | [] -> None
        | x -> Some (e, x) )

  let write_all ?dir (t : t) =
    Core_list.map Event_type.all ~f:(fun e -> (e, write ?dir t e))
end

module Writer (Csvable : CSVABLE) : WRITER with module Csvable = Csvable =
struct
  module Csvable = Csvable

  type t = Csvable.t list

  let empty : t = []

  let add (t : t) (data : Csvable.t) : t = t @ [ data ]

  let add' (t : t) (data : Csvable.t list) : t = t @ data

  let of_list = Fn.id

  let csv_header (_ : t) = Csvable.csv_header

  let get (t : t) = Core_list.map t ~f:(fun t -> Csvable.row_of_t t)

  let write ?dir ~(name : string) (t : t) : int =
    let dir =
      match dir with
      | None -> Core_unix.getcwd ()
      | Some dir -> dir
    in
    let filename = Filename.concat dir (sprintf "%s.csv" name) in
    let header = csv_header t in
    Out_channel.with_file ~append:true ~binary:false ~fail_if_exists:false
      filename ~f:(fun out ->
        maybe_write_header out filename header;
        let len = Core_list.length t in
        Csvable.csv_save_out out t;
        len )
end
