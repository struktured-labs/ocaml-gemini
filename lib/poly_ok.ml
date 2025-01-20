type 'a ok = [ `Ok of 'a ]

let ok_exn ?(message = "not ok") ?here ?sexp_of_error (x : [> 'a ok ]) =
  match x with
  | `Ok x -> x
  | error -> (
    match sexp_of_error with
    | Some f ->
      failwiths message ~here:(Option.value here ~default:[%here]) error f
    | None -> failwith message )

 let ok_exn' t = ok_exn t
    
let ok_or_none (x : [> 'a ok ]) =
  match x with
  | `Ok x -> Some x
  | _ -> None

let ok_or_error_s (x : [> 'a ok ]) sexp_of_error =
  match x with
  | `Ok x -> Or_error.return x
  | e -> Or_error.error_s (sexp_of_error e)

let ok_or_error (x : [> 'a ok ]) string_of_error =
  match x with
  | `Ok x -> Or_error.return x
  | e -> Or_error.error_string @@ string_of_error e

let ok (x : 'a) : 'a ok = `Ok x
