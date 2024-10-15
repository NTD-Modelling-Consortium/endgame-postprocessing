from functools import reduce
from operator import mul


def probability_any_worm(probability_for_each_worm):
    """
    Calculate the probability of having any worm
    """
    prob_of_not_each_worm = map(
        lambda prob_having_worm: 1.0 - prob_having_worm, probability_for_each_worm
    )
    prob_not_any_worm = reduce(mul, prob_of_not_each_worm, 1.0)
    return 1.0 - prob_not_any_worm
