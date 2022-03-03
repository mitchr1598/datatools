from abc import ABC, abstractmethod
import pandas as pd


class Uploader(ABC):

    @abstractmethod
    def get_successful_uploads(self):
        pass

    @abstractmethod
    def get_failed_uploads(self):
        pass

    @abstractmethod
    def upload_data(self, df):
        pass


class CSVUploader(Uploader):

    def __init__(self, filename):
        self.filename = filename
        if not self.filename.endswith('.csv'):
            self.filename += '.csv'
        self.successful_uploads = pd.DataFrame()
        self.failed_uploads = pd.DataFrame()

    def get_successful_uploads(self):
        return self.successful_uploads

    def get_failed_uploads(self):
        return self.failed_uploads

    def upload_data(self, df):
        df.to_csv(self.filename, index=False)
        pd.concat([self.successful_uploads, df], ignore_index=True)


