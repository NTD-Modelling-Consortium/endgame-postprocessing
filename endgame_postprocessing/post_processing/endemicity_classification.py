import pandas as pd
from endgame_postprocessing.post_processing.disease import Disease


class EndemicityClassifier:

    def __init__(
        self,
        endemic_states: set[str],
        non_endemic_states: set[str],
        missing_data_is_endemic,
    ):
        if endemic_states.intersection(non_endemic_states):
            raise Exception("Overlap between endemic and non-endemic states")
        self.endemic_states = endemic_states
        self.non_endemic_states = non_endemic_states
        self.missing_data_is_endemic = missing_data_is_endemic

    def is_state_endemic(self, state: str) -> bool:
        if pd.isna(state):
            return self.missing_data_is_endemic
        if state not in self._all_states():
            raise Exception(
                f"Invalid endemic state: {state} - must be one of: {self._all_states()}"
            )
        return state in self.endemic_states

    def _all_states(self) -> set[str]:
        return self.endemic_states.union(self.non_endemic_states)


ENDEMICITY_CLASSIFIERS = {
    Disease.ONCHO: EndemicityClassifier(
        endemic_states={
            "Endemic (MDA not delivered)",
            "Endemic (under MDA)",
            "Endemic (under post-intervention surveillance)",
        },
        non_endemic_states={
            "Unknown (under LF MDA)",
            "Unknown (consider Oncho Elimination Mapping)",
            "Non-endemic",
            "Not reported",
            "Endemic (pending IA)",
        },
        missing_data_is_endemic=True,
    ),
    Disease.LF: EndemicityClassifier(
        endemic_states={
            "Endemic (MDA not delivered)",
            "Endemic (under post-intervention surveillance)",
            "Endemic (under MDA)",
            "Endemic (pending IA)",
        },
        non_endemic_states={
            "Non-endemic",
            "Not reported",
            "Endemicity unknown",
        },
        missing_data_is_endemic=True,
    ),
}
