from enum import Enum


class Worm(Enum):
    # STH Worms
    ASCARIS = 1 # aka roundworm
    HOOKWORM = 2
    WHIPWORM = 3 # aka trichuriasis

    # SCH Worms
    HAEMATOBIUM = 4
    MANSONI_HIGH_BURDEN = 5
    MANSONI_LOW_BURDEN = 6
