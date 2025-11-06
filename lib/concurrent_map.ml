open Core
open Async

(** A lightweight, Async-friendly concurrent map.

    This module provides a {!Map}-backed container wrapped in an
    {!Async.Mvar.Read_write.t} so updates are performed atomically via
    {!Mvar.update_exn}. It is designed for single-process, cooperative
    concurrency with Async. It does not provide cross-process safety or
    a lock-free data structure; rather, it offers simple, composable atomic
    updates in the presence of multiple Async jobs.

    Usage highlights:
    - [empty] creates a new concurrent map; [of_map]/[of_alist_exn] wrap an
      existing map.
    - [set]/[add_exn]/[remove]/[clear] mutate the map in-place via
      atomic updates.
    - [map]/[mapi]/[filter]/… (without the [_in_place] suffix) are
      functional: they return a new concurrent map and do not mutate the
      original.
    - [_in_place] variants update the underlying map atomically.
    - [find]/[find_exn]/[keys]/[data]/[iter]/… operate on a stable snapshot
      (the current contents at the time of the call).

    Concurrency semantics:
    - All mutations use [Mvar.update_exn], so each update is atomic with
      respect to other updates.
    - Readers observe the most recent successfully committed map. There is
      no transaction spanning multiple operations; compose atomic updates via
      a single [Mvar.update_exn] when needed.

    Complexity: operations have the same asymptotics as the underlying
    [Core.Map] (typically O(log n) for lookup/update), plus constant overhead
    for the [Mvar] wrapper.
*)

module Make (Key : Map.Key) = struct
  module M = Map.Make (Key)

  type 'a t = 'a M.t Mvar.Read_write.t

  (** The concurrent map type parameterized by value type ['a].
      Internally this is an [Mvar.Read_write.t] containing a [Map]. *)

  (** Wrap an existing map in a concurrent container. *)
  let of_map (m : 'a M.t) : 'a t =
    let mvar = Mvar.create () in
    Mvar.set mvar m;
    mvar

  (** Create from an association list, raising on duplicate keys. *)
  let of_alist_exn l : 'a t = of_map (M.of_alist_exn l)

  (** Return a snapshot of the underlying map (no copying).
      Subsequent mutations will not affect the returned value. *)
  let to_map (t : 'a t) = Mvar.peek_exn t

  (** Create an empty concurrent map. *)
  let empty () : 'a t = of_map M.empty

  (** Atomically add a new binding, raising if the key already exists. *)
  let add_exn (t : 'a t) ~key ~data =
    Mvar.update_exn t ~f:(Map.add_exn ~key ~data)

  (** Atomically set/replace a binding. *)
  let set (t : 'a t) ~key ~data = Mvar.update_exn t ~f:(Map.set ~key ~data)

  (** Atomically remove a key if present. *)
  let remove (t : 'a t) key = Mvar.update_exn t ~f:(fun m -> Map.remove m key)

  (** Atomically clear all bindings. *)
  let clear (t : 'a t) = Mvar.update_exn t ~f:(fun _m -> M.empty)

  (** Return the current set of keys from a snapshot. *)
  let keys (t : 'a t) = to_map t |> Map.keys

  (** Curried lookup against the current snapshot.
      Usage: [find t key] -> ['a option]. *)
  let find (t : 'a t) = to_map t |> Map.find

  (** Lookup that raises if the key is absent, using the current snapshot. *)
  let find_exn t = to_map t |> Map.find_exn

  (** Functional map: produce a new concurrent map by applying [f] to values. *)
  let map t ~f = to_map t |> M.map ~f |> of_map

  (** Functional mapi: produce a new concurrent map with access to keys. *)
  let mapi t ~f = to_map t |> Map.mapi ~f |> of_map

  (** Functional filter: retain only bindings satisfying [f]. *)
  let filter t ~f = to_map t |> Map.filter ~f |> of_map

  (** Functional filteri: retain only bindings satisfying [f] with key access. *)
  let filteri t ~f = to_map t |> Map.filteri ~f |> of_map

  (** Functional filter_map: transform and drop [None]s. *)
  let filter_map t ~f = to_map t |> Map.filter_map ~f |> of_map

  (** Functional filter_mapi: transform with key access and drop [None]s. *)
  let filter_mapi t ~f = to_map t |> Map.filter_mapi ~f |> of_map

  (** Iterate over the current snapshot. *)
  let iter t ~f = to_map t |> Map.iter ~f

  (** Iterate with keys over the current snapshot. *)
  let iteri t ~f = to_map t |> Map.iteri ~f

  (** Curried fold over the current snapshot. *)
  let fold (t : 'a t) = to_map t |> Map.fold

  (** Values from the current snapshot. *)
  let data (t : 'a t) = to_map t |> Map.data

  (** In-place filter: atomically retain only bindings satisfying [f]. *)
  let filter_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filter m ~f)

  (** In-place filter with key access. *)
  let filteri_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filteri m ~f)

  (** In-place map over values. *)
  let map_in_place (t : 'a t) ~f = Mvar.update_exn t ~f:(fun m -> M.map m ~f)

  (** In-place map with key access. *)
  let mapi_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.mapi m ~f)

  (** In-place filter_map: transform and drop [None]s. *)
  let filter_map_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filter_map m ~f)

  (** In-place filter_mapi: transform with key access, dropping [None]s. *)
  let filter_mapi_in_place (t : 'a t) ~f =
    Mvar.update_exn t ~f:(fun m -> Map.filter_mapi m ~f)
end
