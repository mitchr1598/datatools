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


class CSVUploader(Uploader):

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        if not self.filename.endswith('.csv'):
            self.filename += '.csv'

    def upload_data(self, df):
        df.to_csv(self.filename, index=False)
        pd.concat([self._successful_uploads, df], ignore_index=True)


