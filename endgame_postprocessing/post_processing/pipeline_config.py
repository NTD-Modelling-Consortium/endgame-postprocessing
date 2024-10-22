from dataclasses import dataclass
from endgame_postprocessing.post_processing.disease import Disease


@dataclass
class PipelineConfig:
    disease: Disease
    threshold: float = 0.01
