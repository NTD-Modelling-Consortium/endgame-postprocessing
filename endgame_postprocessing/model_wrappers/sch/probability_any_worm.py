from functools import reduce
from operator import mul
from typing import Iterable

import numpy as np


def independent_probability(probability_for_each_worm: Iterable[float]):
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
        lambda prob_having_worm: 1.0 - prob_having_worm, probability_for_each_worm
    )
    prob_not_any_worm = reduce(mul, prob_of_not_each_worm, 1.0)
    return 1.0 - prob_not_any_worm

def max_of_any(probability_for_each_worm: Iterable[float]):
    """
    Calculate the probability of having any worm, given by the highest probability
    among all the worms. Used for SCH.

    Inputs:
     - probability_for_each_worm: Probability of having each worm

    Returns: the probability of having any worm.
    """
    return reduce(lambda x, y: np.maximum(x, y), probability_for_each_worm)
