from abc import ABC, abstractmethod
import pandas as pd


class Uploader(ABC):
    def __init__(self):
        self._successful_uploads = pd.DataFrame()
        self._failed_uploads = pd.DataFrame()

    def get_successful_uploads(self):
        return self._successful_uploads

    def get_failed_uploads(self):
        return self._failed_uploads

    @abstractmethod
    def upload_data(self, df) -> None:
        pass


class DBUploader(Uploader):
    def __init__(self, db_connection, table_name, if_exists='append'):
        # Specifying table_name etc. here to adhere to Liskov Substitution Principle on the use of upload_data method
        super().__init__()
        self._db_connection = db_connection
        self._table_name = table_name
        self._if_exists = if_exists

    def upload_data(self, df) -> None:
        df.to_sql(name=self._table_name, con=self._db_connection, if_exists=self._if_exists, index=False)


class CSVUploader(Uploader):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        if not self.filename.endswith('.csv'):
            self.filename += '.csv'

    def upload_data(self, df):
        df.to_csv(self.filename, index=False)
        self._successful_uploads = pd.concat([self._successful_uploads, df], ignore_index=True)


# Random code generated by copilot. Might be useful
"""
class UploaderFactory:
    @staticmethod
    def get_uploader(uploader_type):
        if uploader_type == 'csv':
            return CSVUploader()
        elif uploader_type == 'json':
            return JSONUploader()
        else:
            raise ValueError('Invalid uploader type')
"""
