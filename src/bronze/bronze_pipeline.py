from pyspark.sql.functions import col


class BronzePipeline:

    def __init__(self, spark, config: dict):
        self.spark = spark
        self.catalog = config["catalog"]
        self.schema = config["schema"]
        self.xml_path = config["xml_path"]
        self.row_tag = config["row_tag"]
        self.tables = config["tables"]

    def create_database(self):
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.schema}"
        )

    def read_xml(self):
        df = (
            self.spark.read.format("xml")
            .option("rowTag", self.row_tag)
            .load(self.xml_path)
        )

        print("=================================")
        print("XML Loaded Successfully")
        print("Total Records:", df.count())
        print("=================================")

        return df

    def transform_device(self, df):
        return df.select(
            col("Device.DeviceID").alias("device_id"),
            col("Device.SerialNumber").alias("serial_number"),
            col("Device.Model").alias("model"),
            col("Device.Manufacturer").alias("manufacturer"),
            col("Device.FirmwareVersion").alias("firmware_version"),
            col("Device.InstallDate").alias("install_date"),
            col("Device.Location.Building").alias("building"),
            col("Device.Location.Floor").alias("floor"),
            col("Device.Location.Department").alias("department"),
            col("Device.Location.Country").alias("country")
        )

    def transform_usage(self, df):
        return df.select(
            col("Device.DeviceID").alias("device_id"),
            col("Usage.TotalPages").alias("total_pages"),
            col("Usage.ColorPages").alias("color_pages"),
            col("Usage.MonoPages").alias("mono_pages"),
            col("Usage.DuplexPages").alias("duplex_pages"),
            col("Usage.LastPrintedTimestamp").alias("last_printed_timestamp")
        )

    def transform_events(self, df):
        return df.select(
            col("Device.DeviceID").alias("device_id"),
            col("Events.Supplies.BlackTonerLevel").alias("black_toner"),
            col("Events.Supplies.CyanTonerLevel").alias("cyan_toner"),
            col("Events.Supplies.MagentaTonerLevel").alias("magenta_toner"),
            col("Events.Supplies.YellowTonerLevel").alias("yellow_toner"),
            col("Events.Errors.Error.ErrorCode").alias("error_code"),
            col("Events.Errors.Error.Severity").alias("severity"),
            col("Events.Errors.Error.Message").alias("error_message"),
            col("Events.Errors.Error.Timestamp").alias("error_timestamp"),
            col("Events.Maintenance.LastServiceDate").alias("last_service_date"),
            col("Events.Maintenance.NextServiceDue").alias("next_service_due"),
            col("Events.Maintenance.ServiceProvider").alias("service_provider")
        )

    def write_table(self, df, table_name):
        full_table = f"{self.catalog}.{self.schema}.{table_name}"

        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table)

        print(f"Table written: {full_table}")

    def validate_tables(self):
        print("\n========== Validation ==========")
        for table in self.tables.values():
            full_table = f"{self.catalog}.{self.schema}.{table}"
            count = self.spark.table(full_table).count()
            print(f"{full_table} → {count} records")

    def run(self):
        self.create_database()

        df_raw = self.read_xml()

        device_df = self.transform_device(df_raw)
        usage_df = self.transform_usage(df_raw)
        events_df = self.transform_events(df_raw)

        self.write_table(device_df, self.tables["bronze_device"])
        self.write_table(usage_df, self.tables["bronze_usage"])
        self.write_table(events_df, self.tables["bronze_events"])

        self.validate_tables()

        print("\nBronze Pipeline Completed Successfully.")
