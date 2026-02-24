class SilverPipeline:

    def __init__(self, spark, config: dict):
        self.spark = spark
        self.catalog = config["catalog"]
        self.schema = config["schema"]
        self.bronze_table = config["bronze_table"]
        self.silver_tables = config["silver_tables"]

        self.full_bronze_table = f"{self.catalog}.{self.schema}.{self.bronze_table}"

    def create_database(self):
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.schema}"
        )

    def read_bronze(self):
        print(f"Reading Bronze table: {self.full_bronze_table}")
        df = self.spark.table(self.full_bronze_table)
        print("Bronze Record Count:", df.count())
        return df

    def write_table(self, df, table_name):
        full_table = f"{self.catalog}.{self.schema}.{table_name}"

        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table)

        print(f"Written Silver table: {full_table}")

    def transform_devices(self, df):
        return df.select(
            "device_id",
            "serial_number",
            "model",
            "manufacturer",
            "firmware_version",
            "install_date",
            "building",
            "floor",
            "department",
            "country"
        ).dropDuplicates(["device_id"])

    def transform_usage(self, df):
        return df.select(
            "device_id",
            "total_pages",
            "color_pages",
            "mono_pages",
            "duplex_pages",
            "last_printed_ts"
        )

    def transform_supplies(self, df):
        return df.select(
            "device_id",
            "black_toner",
            "cyan_toner",
            "magenta_toner",
            "yellow_toner"
        )

    def transform_errors(self, df):
        return df.select(
            "device_id",
            "error_code",
            "error_severity",
            "error_message",
            "error_ts"
        )

    def transform_maintenance(self, df):
        return df.select(
            "device_id",
            "last_service_date",
            "next_service_due",
            "service_provider"
        )

    def validate_tables(self):
        print("\n========== Silver Validation ==========")
        for table in self.silver_tables.values():
            full_table = f"{self.catalog}.{self.schema}.{table}"
            count = self.spark.table(full_table).count()
            print(f"{full_table} → {count} records")

    def run(self):
        self.create_database()

        df_bronze = self.read_bronze()

        self.write_table(
            self.transform_devices(df_bronze),
            self.silver_tables["devices"]
        )

        self.write_table(
            self.transform_usage(df_bronze),
            self.silver_tables["usage"]
        )

        self.write_table(
            self.transform_supplies(df_bronze),
            self.silver_tables["supplies"]
        )

        self.write_table(
            self.transform_errors(df_bronze),
            self.silver_tables["errors"]
        )

        self.write_table(
            self.transform_maintenance(df_bronze),
            self.silver_tables["maintenance"]
        )

        self.validate_tables()

        print("\nSilver Pipeline Completed Successfully.")
