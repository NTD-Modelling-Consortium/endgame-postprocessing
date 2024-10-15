import pandas as pd

default_iu_meta_data = pd.DataFrame(
    {
        "ADMIN0ISO3": ["AAA"] * 2 + ["BBB"] * 2,
        # "ADMIN0_FIP": [], # Not used
        # "IUs_NAME": [],  # Not used
        "IU_CODE": ["AAAXXXX00001", "AAAXXXX00002", "BBBXXXX00003", "BBBXXXX00004"],
        "IU_ID": ["00001", "00002", "00003", "00004"],
        # "Population_Total": [],
        # "Population_SAC": [],
        # "Population_Adult": [],
        # "Encemicity_Oncho": [],
        # "Modelled_Oncho": [],
        # "Priority_Population_Oncho": [],
        # "Encemicity_LF": [],
        # "Modelled_LF": [],
        "Priority_Population_LF": [10000] * 4,
        # "Encemicity_Schisto": [],
        # "Priority_Population_Schisto": [],
        # "Encemicity_STH": [],
        # "Priority_Population_STH": [],
        # "Encemicity_TF": [],
        # "Encemicity_TT": [],
        # "Priority_Population_Trachoma": [],
    }
)

default_iu_meta_data.to_csv("PopulationMetadatafile.csv", index=False)
