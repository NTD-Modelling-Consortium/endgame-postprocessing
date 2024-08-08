import pandas as pd
import pandas.testing as pdt
from pyfakefs.fake_filesystem import FakeFilesystem
from endgame_postprocessing.post_processing import aggregation


def test_aggregate_post_processed_files_empty_directory_empty_dataframe(fs):
    pdt.assert_frame_equal(
        aggregation.aggregate_post_processed_files("empty_directory"), pd.DataFrame()
    )


def test_aggregate_post_processed_files_dir_containing_two_csvs_concatenated(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/b.csv", contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False)
    )
    expected_combined_data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    actual = aggregation.aggregate_post_processed_files("dir")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )


def test_aggregate_post_processed_files_dir_containing_two_csvs_with_mismatched_columns_concatenated(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/b.csv", contents=pd.DataFrame({"C": [3], "D": [4]}).to_csv(index=False)
    )
    expected_combined_data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    actual = aggregation.aggregate_post_processed_files("dir")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )
    # Filter should include only a file


def test_aggregate_post_processed_files_dir_containing_nested_csvs_concatenated(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/nested/b.csv",
        contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False),
    )
    expected_combined_data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    actual = aggregation.aggregate_post_processed_files("dir")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )


def test_aggregate_post_processed_files_dir_containing_two_csvs_filter_set(
    fs: FakeFilesystem,
):
    fs.create_file(
        "dir/a.csv", contents=pd.DataFrame({"A": [1], "B": [2]}).to_csv(index=False)
    )
    fs.create_file(
        "dir/b.csv", contents=pd.DataFrame({"A": [3], "B": [4]}).to_csv(index=False)
    )
    expected_combined_data = pd.DataFrame({"A": [1], "B": [2]})
    actual = aggregation.aggregate_post_processed_files("dir", specific_files="a.csv")
    pdt.assert_frame_equal(
        # Have to manually re-type as the current loading sets these as ints
        actual.astype(dtype={"A": int, "B": int}),
        expected_combined_data,
    )
