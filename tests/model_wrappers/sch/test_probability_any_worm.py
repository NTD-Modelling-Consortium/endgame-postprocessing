from endgame_postprocessing.model_wrappers.sch import probability_any_worm


def test_probability_any_worm_zero_for_all_worms():
    assert probability_any_worm.independent_probability([0.0, 0.0, 0.0]) == 0.0


def test_probability_any_worm_one_for_one_worm():
    assert probability_any_worm.independent_probability([1.0, 0.0, 0.0]) == 1.0


def test_probability_any_worm_half_for_all_worms():
    assert probability_any_worm.independent_probability([0.5, 0.5, 0.5]) == 1.0 - 0.125


def test_probability_any_worm_max_same_prev():
    assert probability_any_worm.max_of_any([0.5, 0.5, 0.5]) == 0.5


def test_probability_any_worm_diff_prev():
    assert probability_any_worm.max_of_any([0.5, 0.7, 0.3]) == 0.7
