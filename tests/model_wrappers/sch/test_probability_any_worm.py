from endgame_postprocessing.model_wrappers.sch import probability_any_worm
from endgame_postprocessing.model_wrappers.sch.sth_worm import STHWorm


def test_probability_any_worm_zero_for_all_worms():
    assert probability_any_worm.independent_probability({
        STHWorm.ASCARIS: 0.0,
        STHWorm.HOOKWORM: 0.0,
        STHWorm.WHIPWORM: 0.0}) == 0.0


def test_probability_any_worm_one_for_one_worm():
    assert probability_any_worm.independent_probability({
        STHWorm.ASCARIS: 1.0,
        STHWorm.HOOKWORM: 0.0,
        STHWorm.WHIPWORM: 0.0}) == 1.0


def test_probability_any_worm_half_for_all_worms():
    assert probability_any_worm.independent_probability({
        STHWorm.ASCARIS: 0.5,
        STHWorm.HOOKWORM: 0.5,
        STHWorm.WHIPWORM: 0.5}) == 1.0 - 0.125


def test_probability_any_worm_max_same_prev():
    assert probability_any_worm.max_of_any({
        STHWorm.ASCARIS: 0.5,
        STHWorm.HOOKWORM: 0.5,
        STHWorm.WHIPWORM: 0.5}) == 0.5


def test_probability_any_worm_diff_prev():
    assert probability_any_worm.max_of_any({
        STHWorm.ASCARIS: 0.5,
        STHWorm.HOOKWORM: 0.7,
        STHWorm.WHIPWORM: 0.3}) == 0.7
