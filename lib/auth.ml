(** An authentication token type. *)
type t =
  [ `Base64 of Cstruct.t
  | Hex.t
  ]

(** Given a base64 payload, produce an authentication token. *)
let of_payload s : [< t > `Base64 ] =
  let base64 = Base64.encode_exn s in
  let cstruct = Cstruct.of_string base64 in
  `Base64 cstruct

(** Produce a string representation of the authentication token. *)
let to_string : [< t ] -> string = function
  | `Base64 cstruct -> Cstruct.to_string cstruct
  | `Hex hex -> hex


let hmac_sha384 ~api_secret (`Base64 payload) : [< t > `Hex ] =
  let key = api_secret in
  let payload = Cstruct.to_bytes payload in
  let digest = Digestif.SHA384.hmac_bytes ~key payload in
  let hex = Digestif.SHA384.to_hex digest in
  `Hex hex


(** Encode an authentication token as a gemini payload with an http header
    encoding. Use module [Cfg] to determine secrets and the api target. *)
let to_headers (module Cfg : Cfg.S) t =
  Cohttp.Header.of_list
    [ ("Content-Type", "text/plain");
      ("Content-Length", "0");
      ("Cache-Control", "no-cache");
      ("X-GEMINI-PAYLOAD", to_string t);
      ("X-GEMINI-APIKEY", Cfg.api_key);
      ( "X-GEMINI-SIGNATURE",
        hmac_sha384 ~api_secret:Cfg.api_secret t |> to_string )
    ]
