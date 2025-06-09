open Core
open Async

module Make (Key : Map.Key) = struct
  module M = Map.Make (Key)

  type 'a t = 'a M.t Mvar.Read_write.t

  let of_map (m : 'a M.t) : 'a t =
    let mvar = Mvar.create () in
    Mvar.set mvar m;
    mvar

  let of_alist_exn l : 'a t = of_map (M.of_alist_exn l)

  let to_map (t : 'a t) = Mvar.peek_exn t

  let empty () : 'a t = of_map M.empty

  let add_exn (t : 'a t) ~key ~data =
    Mvar.update_exn t ~f:(Map.add_exn ~key ~data)

  let set (t : 'a t) ~key ~data = Mvar.update_exn t ~f:(Map.set ~key ~data)

  let remove (t : 'a t) key = Mvar.update_exn t ~f:(fun m -> Map.remove m key)

  let clear (t : 'a t) = Mvar.update_exn t ~f:(fun _m -> M.empty)

  let keys (t : 'a t) = to_map t |> Map.keys

  let find (t : 'a t) = to_map t |> Map.find

  let find_exn t = to_map t |> Map.find_exn

  let map t ~f = to_map t |> M.map ~f |> of_map

  let mapi t ~f = to_map t |> Map.mapi ~f |> of_map

  let filter t ~f = to_map t |> Map.filter ~f |> of_map

  let filteri t ~f = to_map t |> Map.filteri ~f |> of_map

  let filter_map t ~f = to_map t |> Map.filter_map ~f |> of_map

  let filter_mapi t ~f = to_map t |> Map.filter_mapi ~f |> of_map

  let iter t ~f = to_map t |> Map.iter ~f

  let iteri t ~f = to_map t |> Map.iteri ~f

  let fold (t : 'a t) = to_map t |> Map.fold

  let data (t : 'a t) = to_map t |> Map.data

  let filter_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filter m ~f)

  let filteri_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filteri m ~f)

  let map_in_place (t : 'a t) ~f = Mvar.update_exn t ~f:(fun m -> M.map m ~f)

  let mapi_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.mapi m ~f)

  let filter_map_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filter_map m ~f)

  let filter_mapi_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filter_mapi m ~f)
end
