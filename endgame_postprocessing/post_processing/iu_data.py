import itertools
import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Iterator, Dict

import more_itertools
import pandas as pd

from endgame_postprocessing.post_processing.disease import Disease
from endgame_postprocessing.post_processing.endemicity_classification import (
    ENDEMICITY_CLASSIFIERS,
)


def _is_valid_iu_code(iu_code):
    return re.match(r"[A-Z]{3}.{0,5}\d{5}$", iu_code)


def _get_capitalised_disease(disease: Disease):
    if disease is Disease.ONCHO:
        return "Oncho"
    elif disease is Disease.LF:
        return "LF"
    elif disease is Disease.STH:
        return "STH"
    elif disease is Disease.SCH:
        return "Schisto"
    elif disease is Disease.TRACHOMA:
        return "Trachoma"
    raise Exception(f"Invalid disease {disease}")


def _get_priority_population_column_for_disease(disease: Disease):
    return f"Priority_Population_{_get_capitalised_disease(disease)}"


class IUSelectionCriteria(Enum):
    ALL_IUS = 0
    MODELLED_IUS = 1
    ENDEMIC_IUS = 2
    SIMULATED_IUS = 4  # ie the ones this post processing script is running against


class PopulationIterator(ABC):

    @abstractmethod
    def __init__(self) -> Iterator[int]:
        pass

    @abstractmethod
    def get_for_year(self, year: int) -> int:
        pass


class LongitudinalPopulationIterator(PopulationIterator):
    def __init__(self, year_population_map: Dict[int, int]):
        self._year_population_map = year_population_map
        self._years = sorted(year_population_map.keys())

    def __iter__(self) -> Iterator[int]:
        """Returns iterator that yields populations for each year in order"""
        for year in self._years:
            yield self._year_population_map[year]

    def get_for_year(self, year: int) -> int:
        """Returns the population for a given year"""
        if year not in self._year_population_map:
            raise Exception(f"Year {year} not found in population data. Available years: {self._years}.")
        return self._year_population_map[year]


class SimplePopulationIterator(PopulationIterator):
    """Iterator for non-longitudinal data that repeats the same population regardless of year"""

    def __init__(self, population: int):
        self._population = population

    def __iter__(self) -> Iterator[int]:
        """Returns infinite iterator that repeats the same population value"""
        return itertools.repeat(self._population)

    def get_for_year(self, year: int) -> int:
        """Returns the same population regardless of year"""
        return self._population


class IUData:
    """
    A class for managing Implementation Unit (IU) population data and metadata.
    
    This class provides efficient access to population data at different aggregation levels
    (IU, country, and Africa) and handles both longitudinal (time-series) and non-longitudinal
    population data. It includes optimizations for fast lookups through precomputed views.
    
    The class supports filtering IUs based on different selection criteria and provides
    iterators for accessing population data over time.
    
    Attributes:
        disease (Disease): The disease type for which population data is managed.
        input_data (pd.DataFrame): The raw population data DataFrame.
        iu_selection_criteria (IUSelectionCriteria): Criteria for selecting relevant IUs.
        simulated_ius (set[str], optional): Set of IU codes that are being simulated.
        is_longitudinal (bool): Whether the data contains time-series population data.
        
    Data Structure:
        For longitudinal data, the input DataFrame should contain columns:
        - ADMIN0ISO3: Country code
        - IU_CODE: Implementation Unit code
        - Year: Year of the data
        - Priority_Population_{Disease}: Population count for the disease
        
        For non-longitudinal data, the Year column is omitted.
    
    Example:
        ```python
        # Create IUData instance
        iu_data = IUData(
            input_data=population_df,
            disease=Disease.LF,
            iu_selection_criteria=IUSelectionCriteria.ALL_IUS
        )
        
        # Get population for a specific IU
        populations = list(iu_data.get_priority_population_for_iu("AAA00001"))
        
        # Get country-level population
        country_pop = iu_data.get_priority_population_for_country("AAA")
        
        # Get Africa-level population
        africa_pop = iu_data.get_priority_population_for_africa()
        ```
    """

    def __init__(self,
                 input_data: pd.DataFrame,
                 disease: Disease,
                 iu_selection_criteria: IUSelectionCriteria,
                 simulated_ius: set[str] = None):
        """
        Initialize the IUData instance with population data and configuration.
        
        Args:
            input_data (pd.DataFrame): DataFrame containing population data with required columns:
                - ADMIN0ISO3: Country ISO3 code
                - IU_CODE: Implementation Unit code
                - Priority_Population_{Disease}: Population count for the disease
                - Year (optional): Year of data for longitudinal datasets
            disease (Disease): The disease type (LF, ONCHO, STH, SCH, TRACHOMA)
            iu_selection_criteria (IUSelectionCriteria): Criteria for selecting relevant IUs:
                - ALL_IUS: Include all IUs in the dataset
                - MODELLED_IUS: Include only IUs marked as modelled
                - ENDEMIC_IUS: Include only IUs marked as endemic
                - SIMULATED_IUS: Include only IUs in the simulated_ius set
            simulated_ius (set[str], optional): Set of IU codes being simulated.
                Required when iu_selection_criteria is SIMULATED_IUS.
        
        Raises:
            InvalidIUDataFile: If the input data is invalid (missing columns, 
                duplicates, invalid IU codes, etc.)
        """

        self.disease = disease
        self.input_data = input_data
        self.iu_selection_criteria = iu_selection_criteria
        self.simulated_ius = simulated_ius
        self._is_longitudinal = "Year" in input_data.columns
        self._year_range = {}
        if self._is_longitudinal:
            self._year_range = input_data.groupby("IU_CODE")["Year"].agg(["min", "max"]).iloc[0].to_dict()

        if iu_selection_criteria is IUSelectionCriteria.SIMULATED_IUS and not simulated_ius:
            raise InvalidIUDataFile("Simulated IUs must be provided for SIMULATED_IUS selection criteria")

        self._population_column_name = _get_priority_population_column_for_disease(self.disease)
        if self._population_column_name not in input_data.columns:
            raise InvalidIUDataFile(f"No priority population found for disease {self.disease.name}, "
                                    f"expected {self._population_column_name}")

        duplicate_check = ["IU_CODE", "Year"] if self._is_longitudinal else ["IU_CODE"]
        if input_data.duplicated(subset=duplicate_check).any():
            raise InvalidIUDataFile(f"Duplicate {', '.join(duplicate_check)} found")

        if not input_data["IU_CODE"].apply(_is_valid_iu_code).all():
            raise InvalidIUDataFile("IU_CODE contains invalid IU codes")

        self._precompute_views()
        self._create_population_iterators()

    def get_priority_population_for_iu(self, iu_code: str, year: Optional[int] = None) -> Iterator[int]:
        """
        Get priority population for IU. Returns iterator yielding the population counts for the IU.

        For non-longitudinal data:
            - Without year: returns an infinite iterator that repeats the same population for the IU
            - With year: returns a single item iterator yielding the population for the IU regardless of requested year

        For longitudinal data:
            - Without year: returns iterator over all years for the IU
            - With year: returns a single item iterator yielding the population for the IU for the requested year
        """
        if iu_code not in self._population_iterators:
            raise InvalidIUDataFile(f"IU {iu_code} not found in data")

        pop_iterator = self._population_iterators[iu_code]

        if year is not None:
            return more_itertools.always_iterable(pop_iterator.get_for_year(year))

        return iter(pop_iterator)

    def get_priority_population_for_country(self, country_code: str, year: Optional[int] = None) -> Iterator[int]:
        """
        Get the total priority population for a specific country.
        
        This method uses precomputed views for efficient lookups, avoiding the need
        to filter and aggregate data on each call. Returns an iterator for consistency
        with other get_priority_population methods.
        
        Args:
            country_code (str): The ISO3 country code (e.g., "ETH", "NGA")
            year (Optional[int]): Specific year to get population for. If None, returns
                iterator over all available years.
            
        Returns:
            Iterator[int]: Iterator yielding population counts
            
        For non-longitudinal data:
            - Without year: infinite iterator repeating the same population value
            - With year: single-item iterator with the population value
            
        For longitudinal data:
            - Without year: finite iterator over all years in chronological order
            - With year: single-item iterator with the population for that year
                
        Example:
            ```python
            # Non-longitudinal data
            total_pop = next(iu_data.get_priority_population_for_country("ETH"))
            # Returns: 1500000
            
            # Longitudinal data - all years
            yearly_pops = list(iu_data.get_priority_population_for_country("ETH"))
            # Returns: [1400000, 1500000, 1600000]
            
            # Specific year
            pop_2021 = next(iu_data.get_priority_population_for_country("ETH", 2021))
            # Returns: 1500000
            ```
        """
        if year is not None:
            # Return single value for specific year
            if not self.is_longitudinal:
                pop = self._country_population.get(country_code, 0)
            else:
                pop = self._country_population_by_year.get((country_code, year), 0)
            return more_itertools.always_iterable(pop)
        
        if not self.is_longitudinal:
            # Infinite iterator for non-longitudinal data
            return itertools.repeat(self._country_population.get(country_code, 0))

        # Finite iterator over years for longitudinal data
        country_data = []
        for (country, year), pop in sorted(self._country_population_by_year.items()):
            if country == country_code:
                country_data.append((year, pop))
        # Sort by year and return iterator over population values
        return iter(pop for year, pop in sorted(country_data))

    def get_priority_population_for_africa(self, year: Optional[int] = None) -> Iterator[int]:
        """
        Get the total priority population for all of Africa.
        
        This method uses precomputed views for efficient lookups, aggregating
        population data across all countries and IUs. Returns an iterator for consistency
        with other get_priority_population methods.
        
        Args:
            year (Optional[int]): Specific year to get population for. If None, returns
                iterator over all available years.
            
        Returns:
            Iterator[int]: Iterator yielding population counts
            
        For non-longitudinal data:
            - Without year: infinite iterator repeating the same population value
            - With year: single-item iterator with the population value
            
        For longitudinal data:
            - Without year: finite iterator over all years in chronological order
            - With year: single-item iterator with the population for that year
                
        Example:
            ```python
            # Non-longitudinal data
            total_pop = next(iu_data.get_priority_population_for_africa())
            # Returns: 50000000
            
            # Longitudinal data - all years
            yearly_pops = list(iu_data.get_priority_population_for_africa())
            # Returns: [48000000, 50000000, 52000000]
            
            # Specific year
            pop_2021 = next(iu_data.get_priority_population_for_africa(2021))
            # Returns: 50000000
            ```
        """
        if year is not None:
            # Return single value for specific year
            if not self.is_longitudinal:
                pop = self._africa_total_population
            else:
                pop = self._africa_population_by_year.get(year, 0)
            return more_itertools.always_iterable(pop)
        
        if not self.is_longitudinal:
            # Infinite iterator for non-longitudinal data
            return itertools.repeat(self._africa_total_population)
        else:
            # Finite iterator over years for longitudinal data
            # Sort by year and return iterator over population values
            return iter(pop for year, pop in sorted(self._africa_population_by_year.items()))

    def get_total_ius_in_country(self, country_code: str) -> int:
        """
        Get the total number of Implementation Units (IUs) in a specific country.
        
        This method uses precomputed views for efficient lookups. The count represents
        unique IUs and is the same for both longitudinal and non-longitudinal data.
        
        Args:
            country_code (str): The ISO3 country code (e.g., "ETH", "NGA")
            
        Returns:
            int: Number of unique IUs in the country
            
        Example:
            ```python
            iu_count = iu_data.get_total_ius_in_country("ETH")
            # Returns: 142
            ```
        """
        return self._country_iu_count.get(country_code, 0)

    def _get_included_ius_for_country(self, country_code):
        return self.get_included_ius().loc[self.input_data["ADMIN0ISO3"] == country_code]

    def get_included_ius(self):
        if self.iu_selection_criteria == IUSelectionCriteria.ALL_IUS:
            return self.input_data
        if self.iu_selection_criteria == IUSelectionCriteria.MODELLED_IUS:
            return self._get_modelled_ius()
        if self.iu_selection_criteria == IUSelectionCriteria.ENDEMIC_IUS:
            return self._get_endemic_ius()
        if self.iu_selection_criteria == IUSelectionCriteria.SIMULATED_IUS:
            return self._get_simulated_ius()
        raise Exception(f"Invalid IU Selection Criteria {self.iu_selection_criteria}")

    def _precompute_views(self):
        included_ius = self.get_included_ius()

        # Compute IU counts (same for both longitudinal and non-longitudinal)
        unique_ius_by_country = included_ius.drop_duplicates(subset=["ADMIN0ISO3", "IU_CODE"])
        country_iu_counts = unique_ius_by_country.groupby("ADMIN0ISO3").size()
        self._country_iu_count = country_iu_counts.to_dict()
        self._africa_iu_count = included_ius["IU_CODE"].nunique()

        # Build grouping columns based on data type
        group_cols = ["ADMIN0ISO3"]
        if self.is_longitudinal:
            group_cols.append("Year")

        # Single groupby operation
        grouped = included_ius.groupby(group_cols)[self._population_column_name].sum()

        if self.is_longitudinal:
            # Store country-year populations directly
            self._country_population_by_year = grouped.to_dict()

            # Aggregate to year level for Africa
            self._africa_population_by_year = grouped.groupby(level="Year").sum().to_dict()
        else:
            # Store country populations directly
            self._country_population = grouped.to_dict()

            # Africa total population
            self._africa_total_population = grouped.sum()

    def _create_population_iterators(self):
        self._population_iterators: Dict[str, PopulationIterator] = {}
        if self.is_longitudinal:
            # Group by IU_CODE and create longitudinal iterators
            for iu_code, iu_data in self.input_data.groupby("IU_CODE"):
                year_pop_map = dict(zip(iu_data["Year"], iu_data[self._population_column_name]))
                self._population_iterators[iu_code] = LongitudinalPopulationIterator(year_pop_map)
        else:
            # Create non-longitudinal population iterators
            for _, row in self.input_data.iterrows():
                iu_code = row["IU_CODE"]
                population = row[self._population_column_name]
                self._population_iterators[iu_code] = SimplePopulationIterator(population)

    @property
    def is_longitudinal(self):
        """
        Checks if the population data is longitudinal.

        Returns:
            True if the population data contains a `Year` column.
        """
        return self._is_longitudinal
    
    @property
    def year_range(self):
        """
        Get the year range for longitudinal data.
        
        This property returns information about the temporal coverage of the population data.
        For longitudinal data, it provides the minimum and maximum years available in the dataset.
        For non-longitudinal data, it returns an empty dictionary.
        
        Returns:
            dict: Dictionary containing year range information:
                - For longitudinal data: {"min": int, "max": int} representing the earliest and latest years
                - For non-longitudinal data: {} (empty dictionary)
                
        Example:
            ```python
            # Longitudinal data
            iu_data = IUData(longitudinal_df, Disease.LF, IUSelectionCriteria.ALL_IUS)
            year_range = iu_data.year_range
            # Returns: {"min": 1995, "max": 2025}
            
            # Non-longitudinal data
            iu_data = IUData(static_df, Disease.LF, IUSelectionCriteria.ALL_IUS)
            year_range = iu_data.year_range
            # Returns: {}
            ```
        """
        return self._year_range

    def _get_modelled_ius(self):
        modelled_column = self._get_modelled_column_name()
        return self.input_data[self.input_data[modelled_column]]

    def _get_modelled_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        return f"Modelled_{disease_str}"

    def _get_simulated_ius(self):
        return self.input_data.loc[self.input_data["IU_CODE"].isin(self.simulated_ius)]

    def _get_endemic_ius(self):
        endemic_column = self._get_endemic_column_name()
        endemicity_classifier = ENDEMICITY_CLASSIFIERS[self.disease]
        return self.input_data.loc[
            self.input_data[endemic_column].apply(
                endemicity_classifier.is_state_endemic
            )
        ]

    def _get_endemic_column_name(self):
        disease_str = _get_capitalised_disease(self.disease)
        # TODO: typo in the column name - should push into the preprocess step
        return f"Encemicity_{disease_str}"


class InvalidIUDataFile(Exception):
    pass
