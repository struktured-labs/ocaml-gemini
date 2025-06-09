let default_num_values = 1

let default_throttle = Time_float_unix.Span.of_int_ms 500

let default_behavior = `Priority

(** combine ?[consumer] ?[num_values] ?[throttle] ?[behavior] [p1] [p2] mixes
    streams [p1] and [p2] using the provided [behavior], throttling when nothing
    available a span of [throttle] and consuming at most [num_values] per
    iteration. The optional [consumer] is passed as is to both pipe readers.

    [behavior] is one of [`Priority, `Alternate, `Random]. [`Priority] always
    prefers [p1] over [p2] when possible. [`Alternate] alternates the preference
    each iteration. [`Random] randomizes the choice. *)
let combine ?consumer ?(num_values = default_num_values)
    ?(throttle = default_throttle) ?(behavior = default_behavior)
    (p1 : 'a Pipe.Reader.t) (p2 : 'a Pipe.Reader.t) =
  let rec unfolder lst =
    let p1, p2 =
      match behavior with
      | `Priority -> (p1, p2)
      | `Alternate ->
        if lst then
          (p1, p2)
        else
          (p2, p1)
      | `Random ->
        if Random.bool () then
          (p1, p2)
        else
          (p2, p1)
    in
    Log.Global.debug "read_exactly start";
    Pipe.read_now' ~max_queue_length:num_values ?consumer p1 |> function
    | `Ok avail ->
      Log.Global.debug "p1 ok";
      return (Some (Queue.to_list avail, not lst))
    | `Eof
    | `Nothing_available -> (
      Log.Global.debug "p1 eof";
      Pipe.read_now' ~max_queue_length:num_values ?consumer p2 |> function
      | `Ok avail ->
        Log.Global.debug "p2 ok";
        return (Some (Queue.to_list avail, not lst))
      | `Eof ->
        Log.Global.debug "p2 eof";
        return None
      | `Nothing_available ->
        Log.Global.debug "p2 eof";
        Clock.after throttle >>= fun () -> unfolder (not lst) )
  in
  Pipe.unfold ~init:true ~f:unfolder |> Pipe.concat_map_list ~f:Fn.id
