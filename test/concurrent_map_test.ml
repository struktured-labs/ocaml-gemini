open Core
open Async
open Expect_test_helpers_core
open Gemini
module CM = Concurrent_map.Make (Int)

let print_t t =
  let alist = ref [] in
  CM.iteri t ~f:(fun ~key ~data -> alist := (key, data) :: !alist);
  let alist = List.rev !alist in
  print_s [%sexp (alist : (int * string) list)]

let%expect_test "empty and set/find" =
  let t = CM.empty () in
  printf "%d\n" (List.length (CM.keys t));
  [%expect {| 0 |}];
  CM.set t ~key:1 ~data:"a";
  CM.set t ~key:2 ~data:"b";
  print_t t;
  [%expect {|
    ((1 a)
     (2 b))
    |}];
  printf !"%{sexp: string option}\n" (CM.find t 1);
  [%expect {| (a) |}];
  printf !"%{sexp: string option}\n" (CM.find t 3);
  [%expect {| () |}];
  return ()
;;

let%expect_test "remove and clear" =
  let t = CM.of_alist_exn [ 1, "a"; 2, "b"; 3, "c" ] in
  CM.remove t 2;
  print_t t;
  [%expect {|
    ((1 a)
     (3 c))
    |}];
  CM.clear t;
  print_t t;
  [%expect {| () |}];
  return ()
;;

let%expect_test "map/mapi (functional)" =
  let t = CM.of_alist_exn [ 1, "a"; 2, "bb" ] in
  let t' = CM.map t ~f:String.uppercase in
  print_t t;
  [%expect {|
    ((1 a)
     (2 bb))
    |}];
  print_t t';
  [%expect {|
    ((1 A)
     (2 BB))
    |}];
  let t'' = CM.mapi t ~f:(fun ~key ~data -> sprintf "%d:%s" key data) in
  print_t t'';
  [%expect {|
    ((1 1:a)
     (2 2:bb))
    |}];
  return ()
;;

let%expect_test "filter/filteri (functional)" =
  let t = CM.of_alist_exn [ 1, "a"; 2, "bb"; 3, "ccc" ] in
  let t' = CM.filter t ~f:(fun v -> String.length v >= 2) in
  print_t t';
  [%expect {|
    ((2 bb)
     (3 ccc))
    |}];
  let t'' = CM.filteri t ~f:(fun ~key ~data:_ -> key % 2 = 1) in
  print_t t'';
  [%expect {|
    ((1 a)
     (3 ccc))
    |}];
  return ()
;;

let%expect_test "filter_map/filter_mapi (functional)" =
  let t = CM.of_alist_exn [ 1, "a"; 2, "bb"; 3, "ccc" ] in
  let t' = CM.filter_map t ~f:(fun v -> if String.length v = 1 then Some (v ^ v) else None) in
  print_t t';
  [%expect {| ((1 aa)) |}];
  let t'' = CM.filter_mapi t ~f:(fun ~key ~data -> if key = 2 then Some (data ^ "!") else None) in
  print_t t'';
  [%expect {| ((2 bb!)) |}];
  return ()
;;

let%expect_test "in-place variants" =
  let t = CM.of_alist_exn [ 1, "a"; 2, "bb"; 3, "ccc" ] in
  CM.map_in_place t ~f:String.uppercase;
  print_t t;
  [%expect {|
    ((1 A)
     (2 BB)
     (3 CCC))
    |}];
  CM.mapi_in_place t ~f:(fun ~key ~data -> sprintf "%d-%s" key data);
  print_t t;
  [%expect {|
    ((1 1-A)
     (2 2-BB)
     (3 3-CCC))
    |}];
  CM.filter_in_place t ~f:(fun v -> String.length v <= 4);
  print_t t;
  [%expect {|
    ((1 1-A)
     (2 2-BB))
    |}];
  CM.filteri_in_place t ~f:(fun ~key ~data:_ -> key = 2);
  print_t t;
  [%expect {| ((2 2-BB)) |}];
  CM.filter_map_in_place t ~f:(fun _ -> None);
  print_t t;
  [%expect {| () |}];
  return ()
;;
