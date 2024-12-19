
library(pins)

board <- pins::board_folder("pins")

df <- pin_read(board, "simple_pins_asset")

print(df)