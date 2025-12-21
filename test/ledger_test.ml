open Core
open Async
open Expect_test_helpers_core
open Gemini

module Entry = Ledger.Entry
module Symbol = V1.Symbol
module Side = V1.Side

let print_entry ?(fields = `All) (t : Entry.t) =
  match fields with
  | `All -> print_s [%sexp (t : Entry.t)]
  | `Core ->
    print_s
      [%message
        ""
          ~position:(t.position : float)
          ~pnl:(t.pnl : float)
          ~pnl_spot:(t.pnl_spot : float)
          ~notional:(t.notional : float)
          ~spot:(t.spot : float)
          ~running_price:(t.running_price : float)
          ~running_qty:(t.running_qty : float)
          ~cost_basis:(t.cost_basis : float)]
  | `Avg ->
    print_s
      [%message
        ""
          ~avg_buy_price:(t.avg_buy_price : float)
          ~avg_sell_price:(t.avg_sell_price : float)
          ~avg_price:(t.avg_price : float)
          ~total_buy_qty:(t.total_buy_qty : float)
          ~total_sell_qty:(t.total_sell_qty : float)]
  | `Notional ->
    print_s
      [%message
        ""
          ~buy_notional:(t.buy_notional : float)
          ~sell_notional:(t.sell_notional : float)
          ~notional:(t.notional : float)]
;;

let create_test_entry () =
  Entry.create ~symbol:(Symbol.Enum_or_string.of_enum `Btcusd) ~update_time:(Time_float_unix.epoch) ()
;;

(* Test basic buy trade *)
let%expect_test "on_trade: single buy" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.412878-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((position      1)
     (pnl           0)
     (pnl_spot      50000)
     (notional      -50000)
     (spot          50000)
     (running_price 50000)
     (running_qty   1)
     (cost_basis    50000))
    |}];
  print_entry ~fields:`Avg entry;
  [%expect
    {|
    ((avg_buy_price  50000)
     (avg_sell_price 0)
     (avg_price      50000)
     (total_buy_qty  1)
     (total_sell_qty 0))
    |}];
  return ()
;;

(* Test basic sell trade *)
let%expect_test "on_trade: single sell from long position" =
  let entry = create_test_entry () in
  (* First buy to establish position *)
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:2.0 in
  (* Then sell part of position *)
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:1.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 2) (spot 50000) (pnl_spot 100000)
     (notional -100000) (avg_buy_price 50000) (avg_sell_price 0)
     (avg_price 50000) (update_time (2025-12-21 17:23:09.413147-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (50000))
     (side (Buy)) (qty (2)) (package_price (100000)) (buy_notional 100000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 100000) (running_price 50000) (running_qty 2))
    ((symbol BTCUSD) (pnl 4000) (position 1) (spot 52000) (pnl_spot 52000)
     (notional -48000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 50666.666666666664)
     (update_time (2025-12-21 17:23:09.413164-05:00)) (update_source Trade)
     (total_buy_qty 2) (total_sell_qty 1) (price (52000)) (side (Sell)) (qty (1))
     (package_price (52000)) (buy_notional 100000) (sell_notional 52000)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((position      1)
     (pnl           4000)
     (pnl_spot      52000)
     (notional      -48000)
     (spot          52000)
     (running_price 50000)
     (running_qty   1)
     (cost_basis    50000))
    |}];
  return ()
;;

(* Test multiple buys with average price calculation *)
let%expect_test "on_trade: multiple buys - average price" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.on_trade entry ~price:51000.0 ~side:`Buy ~qty:1.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413255-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((symbol BTCUSD) (pnl 1000) (position 2) (spot 51000) (pnl_spot 102000)
     (notional -101000) (avg_buy_price 50500) (avg_sell_price 0)
     (avg_price 50500) (update_time (2025-12-21 17:23:09.413270-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (51000))
     (side (Buy)) (qty (1)) (package_price (51000)) (buy_notional 101000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 101000) (running_price 50500) (running_qty 2))
    ((position      2)
     (pnl           1000)
     (pnl_spot      102000)
     (notional      -101000)
     (spot          51000)
     (running_price 50500)
     (running_qty   2)
     (cost_basis    101000))
    |}];
  print_entry ~fields:`Avg entry;
  [%expect
    {|
    ((avg_buy_price  50500)
     (avg_sell_price 0)
     (avg_price      50500)
     (total_buy_qty  2)
     (total_sell_qty 0))
    |}];
  return ()
;;

(* Test buy then sell with profit *)
let%expect_test "on_trade: buy then sell with profit" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:1.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413407-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((symbol BTCUSD) (pnl 2000) (position 0) (spot 52000) (pnl_spot 0)
     (notional 2000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.413421-05:00))
     (update_source Trade) (total_buy_qty 1) (total_sell_qty 1) (price (52000))
     (side (Sell)) (qty (1)) (package_price (52000)) (buy_notional 50000)
     (sell_notional 52000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 0) (running_price 0) (running_qty 0))
    ((position      0)
     (pnl           2000)
     (pnl_spot      0)
     (notional      2000)
     (spot          52000)
     (running_price 0)
     (running_qty   0)
     (cost_basis    0))
    |}];
  print_entry ~fields:`Avg entry;
  [%expect
    {|
    ((avg_buy_price  50000)
     (avg_sell_price 52000)
     (avg_price      51000)
     (total_buy_qty  1)
     (total_sell_qty 1))
    |}];
  return ()
;;

(* Test buy then sell with loss *)
let%expect_test "on_trade: buy then sell with loss" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.on_trade entry ~price:48000.0 ~side:`Sell ~qty:1.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413507-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((symbol BTCUSD) (pnl -2000) (position 0) (spot 48000) (pnl_spot 0)
     (notional -2000) (avg_buy_price 50000) (avg_sell_price 48000)
     (avg_price 49000) (update_time (2025-12-21 17:23:09.413521-05:00))
     (update_source Trade) (total_buy_qty 1) (total_sell_qty 1) (price (48000))
     (side (Sell)) (qty (1)) (package_price (48000)) (buy_notional 50000)
     (sell_notional 48000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 0) (running_price 0) (running_qty 0))
    ((position      0)
     (pnl           -2000)
     (pnl_spot      0)
     (notional      -2000)
     (spot          48000)
     (running_price 0)
     (running_qty   0)
     (cost_basis    0))
    |}];
  return ()
;;

(* Test partial sell *)
let%expect_test "on_trade: partial sell from position" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:2.0 in
  let entry = Entry.on_trade entry ~price:51000.0 ~side:`Sell ~qty:0.5 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 2) (spot 50000) (pnl_spot 100000)
     (notional -100000) (avg_buy_price 50000) (avg_sell_price 0)
     (avg_price 50000) (update_time (2025-12-21 17:23:09.413594-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (50000))
     (side (Buy)) (qty (2)) (package_price (100000)) (buy_notional 100000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 100000) (running_price 50000) (running_qty 2))
    ((symbol BTCUSD) (pnl 2000) (position 1.5) (spot 51000) (pnl_spot 76500)
     (notional -74500) (avg_buy_price 50000) (avg_sell_price 51000)
     (avg_price 50200) (update_time (2025-12-21 17:23:09.413610-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0.5) (price (51000))
     (side (Sell)) (qty (0.5)) (package_price (25500)) (buy_notional 100000)
     (sell_notional 25500) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 75000) (running_price 50000)
     (running_qty 1.5))
    ((position      1.5)
     (pnl           2000)
     (pnl_spot      76500)
     (notional      -74500)
     (spot          51000)
     (running_price 50000)
     (running_qty   1.5)
     (cost_basis    75000))
    |}];
  return ()
;;

(* Test fees on buy trade *)
let%expect_test "on_trade: buy with fee" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 ~fee_usd:50.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl -50) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50050) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413747-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50050)
     (running_price 50050) (running_qty 1))
    ((position      1)
     (pnl           -50)
     (pnl_spot      50000)
     (notional      -50050)
     (spot          50000)
     (running_price 50050)
     (running_qty   1)
     (cost_basis    50050))
    |}];
  return ()
;;

(* Test fees on sell trade *)
let%expect_test "on_trade: sell with fee" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:1.0 ~fee_usd:52.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413809-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((symbol BTCUSD) (pnl 1948) (position 0) (spot 52000) (pnl_spot 0)
     (notional 1948) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.413823-05:00))
     (update_source Trade) (total_buy_qty 1) (total_sell_qty 1) (price (52000))
     (side (Sell)) (qty (1)) (package_price (52000)) (buy_notional 50000)
     (sell_notional 52000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 0) (running_price 0) (running_qty 0))
    ((position      0)
     (pnl           1948)
     (pnl_spot      0)
     (notional      1948)
     (spot          52000)
     (running_price 0)
     (running_qty   0)
     (cost_basis    0))
    |}];
  return ()
;;

(* Test going short (negative position) - should trigger external trade logic *)
let%expect_test "on_trade: going short triggers external trade" =
  let entry = create_test_entry () in
  (* Sell without a position should create external buy then sell *)
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Sell ~qty:1.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413925-05:00))
     (update_source External_trade) (total_buy_qty 1) (total_sell_qty 0)
     (price (50000)) (side (Buy)) (qty (1)) (package_price (50000))
     (buy_notional 50000) (sell_notional 0) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 50000) (running_price 50000)
     (running_qty 1))
    ((symbol BTCUSD) (pnl 0) (position 0) (spot 50000) (pnl_spot 0) (notional 0)
     (avg_buy_price 50000) (avg_sell_price 50000) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413925-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 1) (price (50000)) (side (Sell)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 50000)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 0)
     (running_price 0) (running_qty 0))
    ((symbol BTCUSD) (pnl 0) (position 0) (spot 50000) (pnl_spot 0) (notional 0)
     (avg_buy_price 50000) (avg_sell_price 50000) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.413925-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 1) (price (50000)) (side (Sell)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 50000)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 0)
     (running_price 0) (running_qty 0))
    ((position      0)
     (pnl           0)
     (pnl_spot      0)
     (notional      0)
     (spot          50000)
     (running_price 0)
     (running_qty   0)
     (cost_basis    0))
    |}];
  return ()
;;

(* Test going short with avg_trade_price *)
let%expect_test "on_trade: going short with avg_trade_price" =
  let entry = create_test_entry () in
  (* Sell without a position, but provide avg_trade_price *)
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:1.0 ~avg_trade_price:50000.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414061-05:00))
     (update_source External_trade) (total_buy_qty 1) (total_sell_qty 0)
     (price (50000)) (side (Buy)) (qty (1)) (package_price (50000))
     (buy_notional 50000) (sell_notional 0) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 50000) (running_price 50000)
     (running_qty 1))
    ((symbol BTCUSD) (pnl 2000) (position 0) (spot 52000) (pnl_spot 0)
     (notional 2000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.414061-05:00))
     (update_source Trade) (total_buy_qty 1) (total_sell_qty 1) (price (52000))
     (side (Sell)) (qty (1)) (package_price (52000)) (buy_notional 50000)
     (sell_notional 52000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 0) (running_price 0) (running_qty 0))
    ((symbol BTCUSD) (pnl 2000) (position 0) (spot 52000) (pnl_spot 0)
     (notional 2000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.414061-05:00))
     (update_source Trade) (total_buy_qty 1) (total_sell_qty 1) (price (52000))
     (side (Sell)) (qty (1)) (package_price (52000)) (buy_notional 50000)
     (sell_notional 52000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 0) (running_price 0) (running_qty 0))
    ((position      0)
     (pnl           2000)
     (pnl_spot      0)
     (notional      2000)
     (spot          52000)
     (running_price 0)
     (running_qty   0)
     (cost_basis    0))
    |}];
  return ()
;;

(* Test notional tracking *)
let%expect_test "on_trade: notional tracking" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  print_entry ~fields:`Notional entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414182-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((buy_notional  50000)
     (sell_notional 0)
     (notional      -50000))
    |}];
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:0.5 in
  print_entry ~fields:`Notional entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 2000) (position 0.5) (spot 52000) (pnl_spot 26000)
     (notional -24000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 50666.666666666664)
     (update_time (2025-12-21 17:23:09.414209-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0.5) (price (52000)) (side (Sell))
     (qty (0.5)) (package_price (26000)) (buy_notional 50000)
     (sell_notional 26000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 25000) (running_price 50000)
     (running_qty 0.5))
    ((buy_notional  50000)
     (sell_notional 26000)
     (notional      -24000))
    |}];
  return ()
;;

(* Test update_spot basic functionality *)
let%expect_test "update_spot: basic spot price update" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.update_spot entry 51000.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414270-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((position      1)
     (pnl           1000)
     (pnl_spot      51000)
     (notional      -50000)
     (spot          51000)
     (running_price 50000)
     (running_qty   1)
     (cost_basis    50000))
    |}];
  return ()
;;

(* Test update_spot with zero position *)
let%expect_test "update_spot: zero position" =
  let entry = create_test_entry () in
  let entry = Entry.update_spot entry 51000.0 in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((position      0)
     (pnl           0)
     (pnl_spot      0)
     (notional      0)
     (spot          51000)
     (running_price 0)
     (running_qty   0)
     (cost_basis    0))
    |}];
  return ()
;;

(* Test update_spot with NaN spot price *)
let%expect_test "update_spot: NaN spot price" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.update_spot entry Float.nan in
  print_entry ~fields:`Core entry;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414368-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((position      1)
     (pnl           -50000)
     (pnl_spot      0)
     (notional      -50000)
     (spot          NAN)
     (running_price 50000)
     (running_qty   1)
     (cost_basis    50000))
    |}];
  return ()
;;

(* Test running price and running qty *)
let%expect_test "on_trade: running price and qty tracking" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  printf "After buy 1 @ 50000: running_price=%f running_qty=%f\n" entry.running_price entry.running_qty;
  [%expect {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414467-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    After buy 1 @ 50000: running_price=50000.000000 running_qty=1.000000
    |}];
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Buy ~qty:1.0 in
  printf "After buy 1 @ 52000: running_price=%f running_qty=%f\n" entry.running_price entry.running_qty;
  [%expect {|
    ((symbol BTCUSD) (pnl 2000) (position 2) (spot 52000) (pnl_spot 104000)
     (notional -102000) (avg_buy_price 51000) (avg_sell_price 0)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.414495-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (52000))
     (side (Buy)) (qty (1)) (package_price (52000)) (buy_notional 102000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 102000) (running_price 51000) (running_qty 2))
    After buy 1 @ 52000: running_price=51000.000000 running_qty=2.000000
    |}];
  let entry = Entry.on_trade entry ~price:53000.0 ~side:`Sell ~qty:1.0 in
  printf "After sell 1 @ 53000: running_price=%f running_qty=%f\n" entry.running_price entry.running_qty;
  [%expect {|
    ((symbol BTCUSD) (pnl 4000) (position 1) (spot 53000) (pnl_spot 53000)
     (notional -49000) (avg_buy_price 51000) (avg_sell_price 53000)
     (avg_price 51666.666666666664)
     (update_time (2025-12-21 17:23:09.414519-05:00)) (update_source Trade)
     (total_buy_qty 2) (total_sell_qty 1) (price (53000)) (side (Sell)) (qty (1))
     (package_price (53000)) (buy_notional 102000) (sell_notional 53000)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 51000)
     (running_price 51000) (running_qty 1))
    After sell 1 @ 53000: running_price=51000.000000 running_qty=1.000000
    |}];
  return ()
;;

(* Test cost basis tracking *)
let%expect_test "on_trade: cost basis with fees" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 ~fee_usd:50.0 in
  printf "After buy with fee: cost_basis=%f\n" entry.cost_basis;
  [%expect {|
    ((symbol BTCUSD) (pnl -50) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50050) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414586-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50050)
     (running_price 50050) (running_qty 1))
    After buy with fee: cost_basis=50050.000000
    |}];
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Buy ~qty:1.0 ~fee_usd:52.0 in
  printf "After second buy with fee: cost_basis=%f\n" entry.cost_basis;
  [%expect {|
    ((symbol BTCUSD) (pnl 1898) (position 2) (spot 52000) (pnl_spot 104000)
     (notional -102102) (avg_buy_price 51000) (avg_sell_price 0)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.414622-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (52000))
     (side (Buy)) (qty (1)) (package_price (52000)) (buy_notional 102000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 102102) (running_price 51051) (running_qty 2))
    After second buy with fee: cost_basis=102102.000000
    |}];
  let entry = Entry.on_trade entry ~price:53000.0 ~side:`Sell ~qty:1.0 ~fee_usd:53.0 in
  printf "After sell with fee: cost_basis=%f\n" entry.cost_basis;
  [%expect {|
    ((symbol BTCUSD) (pnl 3845) (position 1) (spot 53000) (pnl_spot 53000)
     (notional -49155) (avg_buy_price 51000) (avg_sell_price 53000)
     (avg_price 51666.666666666664)
     (update_time (2025-12-21 17:23:09.414654-05:00)) (update_source Trade)
     (total_buy_qty 2) (total_sell_qty 1) (price (53000)) (side (Sell)) (qty (1))
     (package_price (53000)) (buy_notional 102000) (sell_notional 53000)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 51051)
     (running_price 51051) (running_qty 1))
    After sell with fee: cost_basis=51051.000000
    |}];
  return ()
;;

(* Test complex scenario: multiple trades with fees *)
let%expect_test "on_trade: complex scenario with multiple trades and fees" =
  let entry = create_test_entry () in
  (* Buy 2 BTC @ 50000 with $100 fee *)
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:2.0 ~fee_usd:100.0 in
  printf "Step 1 - Buy 2 @ 50000 ($100 fee):\n";
  printf "  position=%f pnl=%f notional=%f running_price=%f cost_basis=%f\n"
    entry.position entry.pnl entry.notional entry.running_price entry.cost_basis;
  [%expect
    {|
    ((symbol BTCUSD) (pnl -100) (position 2) (spot 50000) (pnl_spot 100000)
     (notional -100100) (avg_buy_price 50000) (avg_sell_price 0)
     (avg_price 50000) (update_time (2025-12-21 17:23:09.414723-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (50000))
     (side (Buy)) (qty (2)) (package_price (100000)) (buy_notional 100000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 100100) (running_price 50050) (running_qty 2))
    Step 1 - Buy 2 @ 50000 ($100 fee):
      position=2.000000 pnl=-100.000000 notional=-100100.000000 running_price=50050.000000 cost_basis=100100.000000
    |}];
  (* Buy 1 BTC @ 51000 with $51 fee *)
  let entry = Entry.on_trade entry ~price:51000.0 ~side:`Buy ~qty:1.0 ~fee_usd:51.0 in
  printf "Step 2 - Buy 1 @ 51000 ($51 fee):\n";
  printf "  position=%f pnl=%f notional=%f running_price=%f cost_basis=%f\n"
    entry.position entry.pnl entry.notional entry.running_price entry.cost_basis;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 1849) (position 3) (spot 51000) (pnl_spot 153000)
     (notional -151151) (avg_buy_price 50333.333333333336) (avg_sell_price 0)
     (avg_price 50333.333333333336)
     (update_time (2025-12-21 17:23:09.414751-05:00)) (update_source Trade)
     (total_buy_qty 3) (total_sell_qty 0) (price (51000)) (side (Buy)) (qty (1))
     (package_price (51000)) (buy_notional 151000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 151151) (running_price 50383.666666666664) (running_qty 3))
    Step 2 - Buy 1 @ 51000 ($51 fee):
      position=3.000000 pnl=1849.000000 notional=-151151.000000 running_price=50383.666667 cost_basis=151151.000000
    |}];
  (* Sell 1.5 BTC @ 52000 with $78 fee *)
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:1.5 ~fee_usd:78.0 in
  printf "Step 3 - Sell 1.5 @ 52000 ($78 fee):\n";
  printf "  position=%f pnl=%f notional=%f running_price=%f cost_basis=%f\n"
    entry.position entry.pnl entry.notional entry.running_price entry.cost_basis;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 4771) (position 1.5) (spot 52000) (pnl_spot 78000)
     (notional -73229) (avg_buy_price 50333.333333333336) (avg_sell_price 52000)
     (avg_price 50888.888888888891)
     (update_time (2025-12-21 17:23:09.414781-05:00)) (update_source Trade)
     (total_buy_qty 3) (total_sell_qty 1.5) (price (52000)) (side (Sell))
     (qty (1.5)) (package_price (78000)) (buy_notional 151000)
     (sell_notional 78000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 75575.5) (running_price 50383.666666666664)
     (running_qty 1.5))
    Step 3 - Sell 1.5 @ 52000 ($78 fee):
      position=1.500000 pnl=4771.000000 notional=-73229.000000 running_price=50383.666667 cost_basis=75575.500000
    |}];
  (* Update spot to 53000 *)
  let entry = Entry.update_spot entry 53000.0 in
  printf "Step 4 - Update spot to 53000:\n";
  printf "  position=%f pnl=%f pnl_spot=%f spot=%f\n"
    entry.position entry.pnl entry.pnl_spot entry.spot;
  [%expect
    {|
    Step 4 - Update spot to 53000:
      position=1.500000 pnl=6271.000000 pnl_spot=79500.000000 spot=53000.000000
    |}];
  return ()
;;

(* Test average prices with buys and sells *)
let%expect_test "on_trade: average prices calculation" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  let entry = Entry.on_trade entry ~price:51000.0 ~side:`Buy ~qty:1.0 in
  printf "After 2 buys: avg_buy=%f\n" entry.avg_buy_price;
  [%expect {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.414899-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((symbol BTCUSD) (pnl 1000) (position 2) (spot 51000) (pnl_spot 102000)
     (notional -101000) (avg_buy_price 50500) (avg_sell_price 0)
     (avg_price 50500) (update_time (2025-12-21 17:23:09.414912-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0) (price (51000))
     (side (Buy)) (qty (1)) (package_price (51000)) (buy_notional 101000)
     (sell_notional 0) (total_original 0) (total_executed 0) (total_remaining 0)
     (cost_basis 101000) (running_price 50500) (running_qty 2))
    After 2 buys: avg_buy=50500.000000
    |}];
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:0.5 in
  let entry = Entry.on_trade entry ~price:53000.0 ~side:`Sell ~qty:0.5 in
  printf "After 2 sells: avg_buy=%f avg_sell=%f avg_price=%f\n"
    entry.avg_buy_price entry.avg_sell_price entry.avg_price;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 3000) (position 1.5) (spot 52000) (pnl_spot 78000)
     (notional -75000) (avg_buy_price 50500) (avg_sell_price 52000)
     (avg_price 50800) (update_time (2025-12-21 17:23:09.414938-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 0.5) (price (52000))
     (side (Sell)) (qty (0.5)) (package_price (26000)) (buy_notional 101000)
     (sell_notional 26000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 75750) (running_price 50500)
     (running_qty 1.5))
    ((symbol BTCUSD) (pnl 4500) (position 1) (spot 53000) (pnl_spot 53000)
     (notional -48500) (avg_buy_price 50500) (avg_sell_price 52500)
     (avg_price 51166.666666666664)
     (update_time (2025-12-21 17:23:09.414952-05:00)) (update_source Trade)
     (total_buy_qty 2) (total_sell_qty 1) (price (53000)) (side (Sell))
     (qty (0.5)) (package_price (26500)) (buy_notional 101000)
     (sell_notional 52500) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 50500) (running_price 50500)
     (running_qty 1))
    After 2 sells: avg_buy=50500.000000 avg_sell=52500.000000 avg_price=51166.666667
    |}];
  return ()
;;

(* Test PnL calculation accuracy *)
let%expect_test "on_trade: PnL calculation accuracy" =
  let entry = create_test_entry () in
  (* Buy 1 BTC @ 50000 *)
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  (* Sell 1 BTC @ 52000 (should have +2000 PnL) *)
  let entry = Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:1.0 in
  printf "PnL after complete round trip: %f\n" entry.pnl;
  [%expect {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.415052-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    ((symbol BTCUSD) (pnl 2000) (position 0) (spot 52000) (pnl_spot 0)
     (notional 2000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.415066-05:00))
     (update_source Trade) (total_buy_qty 1) (total_sell_qty 1) (price (52000))
     (side (Sell)) (qty (1)) (package_price (52000)) (buy_notional 50000)
     (sell_notional 52000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 0) (running_price 0) (running_qty 0))
    PnL after complete round trip: 2000.000000
    |}];
  (* Buy 1 BTC @ 51000 *)
  let entry = Entry.on_trade entry ~price:51000.0 ~side:`Buy ~qty:1.0 in
  (* Update spot to 52000 (should add +1000 unrealized PnL) *)
  let entry = Entry.update_spot entry 52000.0 in
  printf "PnL with unrealized gain: %f (notional=%f pnl_spot=%f)\n"
    entry.pnl entry.notional entry.pnl_spot;
  [%expect
    {|
    ((symbol BTCUSD) (pnl 2000) (position 1) (spot 51000) (pnl_spot 51000)
     (notional -49000) (avg_buy_price 50500) (avg_sell_price 52000)
     (avg_price 51000) (update_time (2025-12-21 17:23:09.415102-05:00))
     (update_source Trade) (total_buy_qty 2) (total_sell_qty 1) (price (51000))
     (side (Buy)) (qty (1)) (package_price (51000)) (buy_notional 101000)
     (sell_notional 52000) (total_original 0) (total_executed 0)
     (total_remaining 0) (cost_basis 51000) (running_price 51000)
     (running_qty 1))
    PnL with unrealized gain: 3000.000000 (notional=-49000.000000 pnl_spot=52000.000000)
    |}];
  return ()
;;

(* Test update_source field *)
let%expect_test "on_trade: update_source tracking" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 ~update_source:`Trade in
  printf "After trade: update_source=%s\n"
    (Sexp.to_string (Ledger.Update_source.sexp_of_t entry.update_source));
  [%expect {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.415163-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    After trade: update_source=Trade
    |}];
  let entry = Entry.update_spot entry 51000.0 in
  printf "After update_spot: update_source=%s\n"
    (Sexp.to_string (Ledger.Update_source.sexp_of_t entry.update_source));
  [%expect {| After update_spot: update_source=Market_data |}];
  let entry =
    Entry.on_trade entry ~price:52000.0 ~side:`Sell ~qty:0.5 ~update_source:`External_trade
  in
  printf "After external trade: update_source=%s\n"
    (Sexp.to_string (Ledger.Update_source.sexp_of_t entry.update_source));
  [%expect {|
    ((symbol BTCUSD) (pnl 2000) (position 0.5) (spot 52000) (pnl_spot 26000)
     (notional -24000) (avg_buy_price 50000) (avg_sell_price 52000)
     (avg_price 50666.666666666664)
     (update_time (2025-12-21 17:23:09.415190-05:00))
     (update_source External_trade) (total_buy_qty 1) (total_sell_qty 0.5)
     (price (52000)) (side (Sell)) (qty (0.5)) (package_price (26000))
     (buy_notional 50000) (sell_notional 26000) (total_original 0)
     (total_executed 0) (total_remaining 0) (cost_basis 25000)
     (running_price 50000) (running_qty 0.5))
    After external trade: update_source=External_trade
    |}];
  return ()
;;

(* Test that update_spot clears trade-specific fields *)
let%expect_test "update_spot: clears trade fields" =
  let entry = create_test_entry () in
  let entry = Entry.on_trade entry ~price:50000.0 ~side:`Buy ~qty:1.0 in
  printf "After trade - price: %s, side: %s, qty: %s\n"
    (Option.value_map entry.price ~default:"None" ~f:Float.to_string)
    (Option.value_map entry.side ~default:"None" ~f:Side.to_string)
    (Option.value_map entry.qty ~default:"None" ~f:Float.to_string);
  [%expect {|
    ((symbol BTCUSD) (pnl 0) (position 1) (spot 50000) (pnl_spot 50000)
     (notional -50000) (avg_buy_price 50000) (avg_sell_price 0) (avg_price 50000)
     (update_time (2025-12-21 17:23:09.415297-05:00)) (update_source Trade)
     (total_buy_qty 1) (total_sell_qty 0) (price (50000)) (side (Buy)) (qty (1))
     (package_price (50000)) (buy_notional 50000) (sell_notional 0)
     (total_original 0) (total_executed 0) (total_remaining 0) (cost_basis 50000)
     (running_price 50000) (running_qty 1))
    After trade - price: 50000., side: buy, qty: 1.
    |}];
  let entry = Entry.update_spot entry 51000.0 in
  printf "After update_spot - price: %s, side: %s, qty: %s\n"
    (Option.value_map entry.price ~default:"None" ~f:Float.to_string)
    (Option.value_map entry.side ~default:"None" ~f:Side.to_string)
    (Option.value_map entry.qty ~default:"None" ~f:Float.to_string);
  [%expect {| After update_spot - price: None, side: None, qty: None |}];
  return ()
;;
