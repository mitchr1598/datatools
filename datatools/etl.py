from abc import ABC, abstractmethod
import pandas as pd
import datatools.data_sources
import datatools.uploaders
from typing import Union, Callable
import functools
import datatools.transformers


class ETLBase(ABC):
    """
    Abstract class for ETL processes.
    """
    def __init__(self):
        self.extracted_data: pd.DataFrame = pd.DataFrame()
        self.transformed_data: pd.DataFrame = pd.DataFrame()
        self.uploaded_data: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def extract_data(self, data_source: Union[datatools.data_sources.DataSource, datatools.data_sources.MultiSource],
                     *args, **kwargs) -> None:
        pass

    @abstractmethod
    def transform_data(self, transformer: datatools.transformers.Transformer, args: tuple = None) -> None:
        pass

    @abstractmethod
    def load_data(self, uploader: datatools.uploaders.Uploader) -> None:
        pass


class ETLGeneric(ETLBase):
    def __init__(self):
        super().__init__()
        self.successful_uploads = pd.DataFrame()
        self.failed_uploads = pd.DataFrame()

    def extract_data(self, data_source: datatools.data_sources.DataSource,
                     *args, **kwargs) -> None:
        """
        Extracts data from a data source for later processing.
        :param data_source: Data source to extract data from. It should be already imported.
        :return:
        """
        self.extracted_data = data_source.export_data(*args, **kwargs)

    def transform_data(self, transformer: datatools.transformers.Transformer, args: tuple = None) -> None:
        self.transformed_data = transformer.transform(self.extracted_data)

    def load_data(self, uploader: datatools.uploaders.Uploader) -> None:
        uploader.upload_data(self.transformed_data)
        self.successful_uploads = uploader.get_successful_uploads()
        self.failed_uploads = uploader.get_failed_uploads()





