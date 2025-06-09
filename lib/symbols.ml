open! Common
module Symbol = Symbol

let command : string * Command.t =
  let operation_name = "symbols" in
  let open Command.Let_syntax in
  ( operation_name,
    Command.async
      ~summary:(Path.to_summary ~has_subnames:false [ operation_name ])
      ~readme:(fun () ->
        "List all known currency pairs in this library, separated by newlines." )
      [%map_open
        let _config = Cfg.param in
        fun () ->
          let f s = Print.print_endline s in
          let () =
            List.map ~f:Symbol.to_string Symbol.Enum.all |> List.iter ~f
          in
          Deferred.unit] )
