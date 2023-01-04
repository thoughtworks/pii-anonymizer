import pandas as pd
import dask.dataframe as dd


def standalone_should_match_snapshot():
    expect = pd.read_csv("./e2e/expected.csv")
    actual = pd.read_csv("./e2e/output/standalone.csv")
    pd.testing.assert_frame_equal(expect, actual)
    print("E2E - Standalone test passed")


def spark_should_match_snapshot():
    expect = pd.read_csv("./e2e/expected.csv").reset_index(drop=True)
    actual = dd.read_csv("./e2e/output/spark/*.csv").compute().reset_index(drop=True)
    pd.testing.assert_frame_equal(expect, actual)
    print("E2E - Spark test passed")


standalone_should_match_snapshot()
spark_should_match_snapshot()
