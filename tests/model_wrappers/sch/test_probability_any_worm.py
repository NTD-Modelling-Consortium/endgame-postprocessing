import pytest
from endgame_postprocessing.model_wrappers.sch import probability_any_worm
from endgame_postprocessing.model_wrappers.sch.worm import Worm


def test_probability_any_worm_zero_for_all_worms():
    assert probability_any_worm.independent_probability({
        Worm.ASCARIS: 0.0,
        Worm.HOOKWORM: 0.0,
        Worm.WHIPWORM: 0.0}) == 0.0


def test_probability_any_worm_one_for_one_worm():
    assert probability_any_worm.independent_probability({
        Worm.ASCARIS: 1.0,
        Worm.HOOKWORM: 0.0,
        Worm.WHIPWORM: 0.0}) == 1.0


def test_probability_any_worm_half_for_all_worms():
    assert probability_any_worm.independent_probability({
        Worm.ASCARIS: 0.5,
        Worm.HOOKWORM: 0.5,
        Worm.WHIPWORM: 0.5}) == 1.0 - 0.125


def test_probability_any_worm_max_same_prev():
    assert probability_any_worm.max_of_any({
        Worm.ASCARIS: 0.5,
        Worm.HOOKWORM: 0.5,
        Worm.WHIPWORM: 0.5}) == 0.5


def test_probability_any_worm_diff_prev():
    assert probability_any_worm.max_of_any({
        Worm.ASCARIS: 0.5,
        Worm.HOOKWORM: 0.7,
        Worm.WHIPWORM: 0.3}) == 0.7


def test_probability_any_worm_weighted_regression():
    assert probability_any_worm.linear_model({
        Worm.ASCARIS: 0.5,
        Worm.HOOKWORM: 0.7,
        Worm.WHIPWORM: 0.3}) == pytest.approx(1.43352825)

def test_probability_any_worm_only_one_worm_raises_exception():
    with pytest.raises(ValueError):
        probability_any_worm.linear_model({Worm.ASCARIS: 0.5})
