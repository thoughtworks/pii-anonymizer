import pandas as pd


def standalone_should_match_snapshot():
    expect = pd.read_csv("./e2e/expected.csv")
    actual = pd.read_csv("./e2e/output/standalone.csv")
    pd.testing.assert_frame_equal(expect, actual)


def spark_should_match_snapshot():
    expect = pd.read_csv("./e2e/expected.csv")
    actual = pd.read_csv("./e2e/output/spark.csv")
    pd.testing.assert_frame_equal(expect, actual)


standalone_should_match_snapshot()
print("E2E - Standalone test passed")

spark_should_match_snapshot()
print("E2E - Spark test passed")
