from functools import reduce
from operator import mul
from typing import Callable

import numpy as np

from endgame_postprocessing.model_wrappers.sch.worm import Worm

WormCombinationFunction = Callable[[dict[Worm, float]], float]

def independent_probability(probability_for_each_worm: dict[Worm, float]):
    """
    Calculate the probability of having any worm, given probability of
    having each worm.

    This assumes that the probability of each worm is statistically independent.
    Then via de Morgans law we can get the prob of any worm by working out the prob
    of no worm.

    Inputs:
     - probability_for_each_worm: Probability of having each worm

    Returns: the probability of having any worm.
    """
    prob_of_not_each_worm = map(
        lambda prob_having_worm: 1.0 - prob_having_worm, probability_for_each_worm.values()
    )
    prob_not_any_worm = reduce(mul, prob_of_not_each_worm, 1.0)
    return 1.0 - prob_not_any_worm

def max_of_any(probability_for_each_worm: dict[Worm, float]):
    """
    Calculate the probability of having any worm, given by the highest probability
    among all the worms. Used for SCH.

    Inputs:
     - probability_for_each_worm: Probability of having each worm

    Returns: the probability of having any worm.
    """
    return reduce(lambda x, y: np.maximum(x, y), probability_for_each_worm.values())

def linear_model(probability_for_each_worm: dict[Worm, float]):
    """
    Calculate the probability of having any worm by using a generalized linear
    model to estimate the correlation between the different worms
    """
    required_worms = {Worm.ASCARIS, Worm.HOOKWORM, Worm.WHIPWORM}
    missing_worms = required_worms - probability_for_each_worm.keys()
    if len(missing_worms) > 0:
        raise ValueError(f"Missing worm prevalence: {missing_worms}")

    Asc = probability_for_each_worm[Worm.ASCARIS]
    Hk = probability_for_each_worm[Worm.HOOKWORM]
    TT = probability_for_each_worm[Worm.WHIPWORM]
    return (
        1.10078 +
        (0.41845 * Asc) +
        (0.05568 * Hk) +
        (0.28897 * TT) +
        (-0.07097 * Asc * Hk) +
        (0.10423 * Asc * TT) +
        (0.01396 * Hk * TT) +
        (0.03933 * Asc * Hk * TT))
