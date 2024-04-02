(** Json utility functions. *)

(** Result specification when deserializing raw json into something in strongly
    typed ocaml. *)
module Result = struct
  include Result

  (** Unify results into tuple [('a , 'b)] if both are ok, otherwise produce one
      of the errors . *)
  let both x y : ('a * 'b, 'error) t =
    match x with
    | Result.Ok x -> (
      match y with
      | Result.Ok y -> Result.Ok (x, y)
      | Result.Error _ as e -> e )
    | Result.Error _ as e -> e

  (** Evaluates [f err] if [t] is an error, t otherwise. *)
  let on_error (t : ('a, 'err) t) ~(f : 'err -> ('a, 'err) t) =
    match t with
    | Result.Ok _ as ok -> ok
    | Result.Error err -> f err
end

module type CSVABLE = sig
  type t

  val is_csv_atom : bool

  val rev_csv_header' : string list -> 'a -> 'b -> string list

  val rev_csv_header_spec' :
    Csvfields.Csv.Spec.t list -> 'a -> 'b -> Csvfields.Csv.Spec.t list

  val t_of_row' : 'a -> string list -> (unit -> t) * string list

  val write_row_of_t' :
    is_first:bool ->
    is_last:bool ->
    writer:(string -> unit) ->
    'a ->
    'b ->
    t ->
    unit

  val csv_header : string list

  val csv_header_spec : Csvfields.Csv.Spec.t list

  val t_of_row : string list -> t

  val row_of_t : t -> string list

  val csv_load : ?separator:char -> string -> t list

  val csv_load_in : ?separator:char -> In_channel.t -> t list

  val csv_save_fn : ?separator:char -> (string -> unit) -> t list -> unit

  val csv_save_out : ?separator:char -> Out_channel.t -> t list -> unit

  val csv_save : ?separator:char -> string -> t list -> unit
end

module type S = sig
  type t [@@deriving yojson, enumerate, sexp, compare, equal]

  (** Wraps a string value promoting it to type [Enum enum] if it is parsable as
      such, [`String string] otherwise. *)
  module Enum_or_string : sig
    type enum = t [@@deriving enumerate, sexp, compare, equal]

    type t =
      | Enum of enum
      | String of string
    [@@deriving yojson, sexp, compare, equal]

    include Csvfields.Csv.Stringable with type t := t

    include CSVABLE with type t := t

    val to_enum : t -> enum option

    val of_enum : enum -> t

    val promote : t -> enum option

    val promote_exn : t -> enum

    val to_enum_exn : t -> enum
  end

  val dict : (string * t) list

  val of_string_opt : string -> t option

  val error_message : string -> string

  include Csvfields.Csv.Stringable with type t := t

  include CSVABLE with type t := t
end

(** An enumeration encodable as a json string. *)
module type ENUM_STRING = sig
  type t [@@deriving enumerate, sexp, compare, equal]

  val to_string : t -> string
end

module Enum (T : sig
  type t [@@deriving enumerate, sexp, compare, equal]
end) : ENUM_STRING with type t = T.t = struct
  include T

  let to_string t =
    sexp_of_t t |> function
    | Sexp.Atom s -> s
    | sexp ->
      failwiths ~here:[%here] "Expected string atom but got: %s" sexp Fn.id
end

module Enum_with_overrides (T : sig
  type t [@@deriving enumerate, sexp, equal, compare, equal]

  val overrides : (t, string) List.Assoc.t
end) : ENUM_STRING with type t = T.t = struct
  include T
  include Enum (T)

  let to_string t =
    List.Assoc.find overrides ~equal:T.equal t |> function
    | Some s -> s
    | None -> to_string t
end

(** Produce json encoders and decoders given an enumerated type and its string
    representations. *)
module Make (E : ENUM_STRING) : S with type t = E.t = struct
  module T = struct
    let dict = List.zip_exn (List.map E.all ~f:E.to_string) E.all

    let of_string_opt (s : string) : E.t option =
      List.Assoc.find dict s ~equal:(fun s s' ->
          String.equal (String.lowercase s) (String.lowercase s') )

    let error_message x =
      sprintf "String %S is not a valid enumeration value. Expected one of %s" x
        (String.concat ~sep:", " (List.map ~f:fst dict))

    let of_string x =
      of_string_opt x |> Option.value_exn ~message:(error_message x)

    let to_yojson t = `String (E.to_string t)

    let of_yojson (json : Yojson.Safe.t) =
      match json with
      | `String s -> (
        match of_string_opt s with
        | Some t -> Result.return t
        | None -> error_message s |> Result.fail )
      | #Yojson.Safe.t ->
        Result.failf "expected json string but got %S"
          (Yojson.Safe.to_string json)

    include E

    module Enum_or_string = struct
      module T = struct
        type enum = t [@@deriving enumerate, yojson, sexp, compare, equal]

        type t =
          | Enum of enum
          | String of string
        [@@deriving yojson, sexp, compare, equal]

        let enum_string_of_yojson (json : Yojson.Safe.t) (_ : 'err) :
            (t, string) result =
          match json with
          | `String s -> Result.Ok (String s)
          | #Yojson.Safe.t ->
            Result.failf "expected json string but got %S"
              (Yojson.Safe.to_string json)

        let enum_of_yojson json =
          enum_of_yojson json |> Result.map ~f:(fun x -> (Enum x :> t))

        let of_yojson (json : Yojson.Safe.t) :
            t Ppx_deriving_yojson_runtime.error_or =
          Result.on_error (enum_of_yojson json) ~f:(enum_string_of_yojson json)

        let to_yojson (t : t) : Yojson.Safe.t =
          match t with
          | Enum enum -> enum_to_yojson enum
          | String s -> `String s

        let t_of_sexp sexp : t =
          match sexp with
          | Sexp.Atom s ->
            Option.value_map (of_string_opt s)
              ~f:(fun s -> Enum s)
              ~default:(String (String.uppercase s))
          | sexp ->
            failwiths ~here:[%here]
              "invalid sexp expression for enum_or_string type. expected atom \
               but got:"
              sexp Fn.id

        let to_string x =
          match x with
          | Enum x -> to_string x
          | String s -> s

        let sexp_of_t t : Sexp.t =
          to_string t |> String.uppercase |> fun s -> Sexp.Atom s

        let to_enum = function
          | Enum t -> Some t
          | String _ -> None

        let conversion_error s = sprintf "Can't convert string %s to enum" s

        let to_enum_exn = function
          | Enum t -> t
          | String s -> failwith @@ conversion_error s

        let of_enum e = Enum e

        let equal t t' = equal t t' || String.equal (to_string t) (to_string t')

        let of_string s =
          of_string_opt s |> Option.value_map ~f:of_enum ~default:(String s)

        let promote t =
          match t with
          | String s -> of_string_opt s
          | Enum e -> Some e

        let promote_exn t =
          match t with
          | String s as ss ->
            promote ss
            |> Option.value_exn ~here:[%here] ~message:(conversion_error s)
          | Enum e -> e
      end

      include T

      include (Csvfields.Csv.Atom (T) : Csvfields.Csv.Csvable with type t := t)
    end
  end

  include T

  include (Csvfields.Csv.Atom (T) : Csvfields.Csv.Csvable with type t := t)
end
