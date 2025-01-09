import warnings


def produce_generation_metadata(*,
                                warnings: list[warnings.WarningMessage]):
    return {
        "warnings": warnings
    }
