open Common
module Auth = Auth
module Cfg = Cfg
module Nonce = Nonce
module Rest = Rest
module Result = Json.Result
module Inf_pipe = Inf_pipe
module Poly_ok = Poly_ok
module V1 = V1
module Order_book = Order_book
module Pnl = Pnl
open V1

let command : Command.t =
  Command.group ~summary:"Gemini Command System"
    [ Heartbeat.command;
      Order.command;
      Orders.command;
      Mytrades.command;
      Tradevolume.command;
      Balances.command;
      Market_data.command;
      Order_events.command;
      Notional_volume.command;
      Order_book.command;
      Pnl.command
    ]
