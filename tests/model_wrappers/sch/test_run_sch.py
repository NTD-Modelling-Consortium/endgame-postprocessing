from endgame_postprocessing.model_wrappers.sch.run_sch import probability_any_worm


def test_probability_any_worm_zero_for_all_worms():
    assert probability_any_worm([0.0, 0.0, 0.0]) == 0.0


def test_probability_any_worm_one_for_one_worm():
    assert probability_any_worm([1.0, 0.0, 0.0]) == 1.0


def test_probability_any_worm_half_for_all_worms():
    assert probability_any_worm([0.5, 0.5, 0.5]) == 1.0 - 0.125
