import datetime as dt

from dagster import ConfigurableResource


class ChargyConfig(ConfigurableResource):
    chargy_url: str
    chargy_bucket: str
    kml_key_refix: str
    kml_daily_archive_key_prefix: str
    bronze_prefix: str
    postgresql_start_date: str
    postgresql_end_date: str

    @staticmethod
    def to_date(prm: str) -> dt.date:
        return dt.datetime.strptime(prm, "%Y-%m-%d").date()

    def get_kml_start_date(self) -> dt.date:
        return self.to_date(self.postgresql_end_date) + dt.timedelta(days=1)
