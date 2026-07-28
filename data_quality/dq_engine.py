import os
import pyspark.sql.functions as f
from pyspark.sql.window import Window


class DataQualityFramework:

    def add_metadata(self, df, check_type, failed_column=None):
        return (
            df.withColumn("run_id", f.lit(os.environ.get("run_id")))
            .withColumn("process_id", f.lit(os.environ.get("process_id")))
            .withColumn("check_type", f.lit(check_type))
            .withColumn("failed_column", f.lit(failed_column))
        )

    def NullCheck(self, records, cols):

        records = records.withColumn("source", f.input_file_name())

        result = None

        for col_nm in cols:

            failed = records.filter(f.col(col_nm).isNull()).withColumn(
                "failed_record", f.concat_ws("|", *records.columns)
            )

            failed = self.add_metadata(
                failed, check_type="null_check", failed_column=col_nm
            )

            if result is None:

                result = failed

            else:

                result = result.unionByName(failed)

        return result.distinct()

    def CompletenessCheck(self, records, reqd_cols):

        condition = None

        for c in reqd_cols:

            expr = f.col(c).isNull() | (f.trim(f.col(c)) == "")

            if condition is None:

                condition = expr

            else:

                condition = condition | expr

        failed = (
            records.withColumn("source", f.input_file_name())
            .filter(condition)
            .withColumn("failed_record", f.concat_ws("|", *records.columns))
        )

        return self.add_metadata(failed, check_type="completeness_check")

    def UniquenessCheck(self, records, key_cols):

        window_spec = Window.partitionBy(*key_cols)

        failed = (
            records.withColumn("source", f.input_file_name())
            .withColumn("dup_count", f.count("*").over(window_spec))
            .filter(f.col("dup_count") > 1)
            .drop("dup_count")
            .withColumn("failed_record", f.concat_ws("|", *records.columns))
        )

        return self.add_metadata(
            failed, check_type="uniqueness_check", failed_column=",".join(key_cols)
        )
