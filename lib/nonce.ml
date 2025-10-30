type reader = int Inf_pipe.Reader.t

module type S = sig
  type t [@@deriving sexp]

  val pipe : init:t -> unit -> int Inf_pipe.Reader.t Deferred.t
end

module Counter : S with type t = int = struct
  type t = int [@@deriving sexp]

  let pipe ~init () =
    Inf_pipe.unfold ~init ~f:(fun s ->
        let s' = s + 1 in
        (s, s') |> return )
    |> return
end

module File = struct
  let create_nonce_file ?(default = 0) filename =
    Unix.mkdir ~p:() (Filename.dirname filename) ~perm:0o700 >>= fun () ->
    try_with ~extract_exn:true (fun () ->
        Unix.with_file ~mode:[ `Rdonly ] filename ~f:(fun _fd -> Deferred.unit) )
    >>= function
    | Result.Ok _ -> Deferred.unit
    | Result.Error _ -> Writer.save ~contents:(sprintf "%d\n" default) filename

  type t = string [@@deriving sexp]

  (* TODO - too expensive- only check the file once, then do
   * everything in memory *)
  let pipe ~init:filename () =
    Cfg.create_config_dir () >>= fun () ->
    create_nonce_file ?default:None filename >>= fun () ->
    Inf_pipe.unfold ~init:() ~f:(fun _ ->
        (Reader.open_file ?buf_len:None filename
         >>= Reader.really_read_line ~wait_time:(Time_float.Span.of_ms 1.0)
         >>= function
         | None -> return 0
         | Some nonce -> return @@ Int.of_string nonce )
        >>= fun nonce ->
        let nonce' = nonce + 1 in
        Writer.save filename ~contents:(sprintf "%d\n" nonce') >>= fun () ->
        return (nonce, ()) )
    |> return

  let default_filename =
    let root_path = Unix.getenv_exn "HOME" in
    sprintf "%s/.gemini/nonce.txt" root_path

  let default = pipe ~init:default_filename
end

let _assert_module_file_is_nonce =
  let module F : S with type t = string = File in
  ()

module Request = struct
  type request_nonce =
    { request : string;
      nonce : int
    }
  [@@deriving sexp, yojson]

  type t =
    { request : string;
      nonce : int;
      payload : Yojson.Safe.t option [@default None]
    }

  let make ~request ~nonce ?payload () =
    Inf_pipe.read nonce >>= fun nonce -> return { request; nonce; payload }

  let to_yojson { request; nonce; payload } : Yojson.Safe.t =
    match request_nonce_to_yojson { request; nonce } with
    | `Assoc assoc as a -> (
      match Option.value ~default:`Null payload with
      | `Null -> a
      | `Assoc assoc' -> `Assoc (assoc @ assoc')
      | #Yojson.Safe.t as unsupported_yojson ->
        failwithf "expected json association for request payload but got %S"
          (Yojson.Safe.to_string unsupported_yojson)
          () )
    | #Yojson.Safe.t as unsupported_yojson ->
      failwithf "expected json association for type request_nonce but got %S"
        (Yojson.Safe.to_string unsupported_yojson)
        ()
end
