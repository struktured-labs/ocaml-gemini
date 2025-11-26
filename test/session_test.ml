open Async
open Gemini

(* Test the actual decimal place calculation logic extracted from Session.format_price *)
let calc_decimal_places tick_size =
  Session.calc_decimal_places_from tick_size

let%expect_test "decimal place calculation from tick_size" =
  (* tick_size >= 1.0 should give 0 decimals *)
  printf "%d\n" (calc_decimal_places 1.0);
  [%expect {| 0 |}];
  
  printf "%d\n" (calc_decimal_places 10.0);
  [%expect {| 0 |}];
  
  (* tick_size = 0.01 should give 2 decimals *)
  printf "%d\n" (calc_decimal_places 0.01);
  [%expect {| 2 |}];
  
  (* tick_size = 0.0001 should give 4 decimals *)
  printf "%d\n" (calc_decimal_places 0.0001);
  [%expect {| 4 |}];
  
  (* tick_size = 0.000000001 should give 9 decimals *)
  printf "%d\n" (calc_decimal_places 0.000000001);
  [%expect {| 9 |}];
  
  (* tick_size = 0.1 should give 1 decimal *)
  printf "%d\n" (calc_decimal_places 0.1);
  [%expect {| 1 |}];
  
  return ()

let%expect_test "price formatting with different tick sizes" =
  (* Test BTC-like formatting (tick_size = 1.0) *)
  let decimals = calc_decimal_places 1.0 in
  printf "%.*f\n" decimals 43250.6789;
  [%expect {| 43251 |}];
  
  (* Test ETH-like formatting (tick_size = 0.01) *)
  let decimals = calc_decimal_places 0.01 in
  printf "%.*f\n" decimals 3456.789;
  [%expect {| 3456.79 |}];
  
  (* Test micro-crypto formatting (tick_size = 0.000000001) *)
  let decimals = calc_decimal_places 0.000000001 in
  printf "%.*f\n" decimals 0.000004859;
  [%expect {| 0.000004859 |}];
  
  printf "%.*f\n" decimals 0.0000048591234;
  [%expect {| 0.000004859 |}];
  
  (* Test DOGE-like formatting (tick_size = 0.0001) *)
  let decimals = calc_decimal_places 0.0001 in
  printf "%.*f\n" decimals 0.12345678;
  [%expect {| 0.1235 |}];
  
  return ()

let%expect_test "edge cases" =
  let decimals = calc_decimal_places 0.01 in
  
  (* Test zero *)
  printf "%.*f\n" decimals 0.0;
  [%expect {| 0.00 |}];
  
  (* Test very large number *)
  printf "%.*f\n" decimals 999999.99;
  [%expect {| 999999.99 |}];
  
  (* Test negative *)
  printf "%.*f\n" decimals (-123.45);
  [%expect {| -123.45 |}];
  
  (* Test exact value *)
  printf "%.*f\n" decimals 3456.12;
  [%expect {| 3456.12 |}];
  
  return ()

let%expect_test "various tick_size values" =
  (* Test tick_size = 0.00001 (5 decimals) *)
  let decimals = calc_decimal_places 0.00001 in
  printf "%.*f\n" decimals 1.234567;
  [%expect {| 1.23457 |}];
  
  (* Test tick_size = 0.5 (1 decimal) *)
  let decimals = calc_decimal_places 0.5 in
  printf "%.*f\n" decimals 99.87;
  [%expect {| 99.9 |}];
  
  (* Test tick_size = 0.25 (1 decimal) *)
  let decimals = calc_decimal_places 0.25 in
  printf "%.*f\n" decimals 99.876;
  [%expect {| 99.9 |}];
  
  return ()
