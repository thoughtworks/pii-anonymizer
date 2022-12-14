import pandas as pd
import dask.dataframe as dd


def standalone_should_match_snapshot():
    expect = pd.read_csv("./e2e/expected.csv")
    actual = pd.read_csv("./e2e/output/standalone.csv")
    pd.testing.assert_frame_equal(expect, actual)


def spark_should_match_snapshot():
    expect = pd.read_csv("./e2e/expected.csv")
    actual = dd.read_csv("./e2e/output/spark/*.csv").compute()
    pd.testing.assert_frame_equal(expect, actual)


standalone_should_match_snapshot()
print("E2E - Standalone test passed")

spark_should_match_snapshot()
print("E2E - Spark test passed")
