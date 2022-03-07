import pandas as pd
from abc import ABC, abstractmethod
import requests
import json
import utils
import uploading
import transforming
from typing import Union


class DataSource(ABC):
    """
    Abstract class for importing, transforming, and exporting data from and too various sources processes.
    """
    def __init__(self):
        self.imported_data = None
        self.transformed_data: pd.DataFrame = pd.DataFrame()
        self.exported_data: pd.DataFrame = pd.DataFrame()
        self.failed_exports: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def import_data(self, *args, **kwargs) -> None:
        pass

    def transform_data(self, transformer: transforming.transformers.Transformer = transforming.
                       transformers.Transformer(), *args, **kwargs) -> None:
        self.transformed_data = transformer.transform(self.imported_data)

    def export_data(self, uploader: uploading.uploaders.Uploader = None, *args, **kwargs) -> pd.DataFrame:
        if self.transformed_data.empty:
            self.transform_data()
        if uploader is not None:
            uploader.upload_data(self.transformed_data)
        self.exported_data, self.failed_exports = uploader.get_successful_uploads(), uploader.get_failed_uploads()
        return self.exported_data


class DataframeSource(DataSource):
    def __init__(self):
        super().__init__()

    def import_data(self, df) -> None:
        self.imported_data = df


class JsonSource(DataSource):
    def __init__(self, file_path: str = None):
        super().__init__()
        self.file_path = file_path

    def import_data(self, data: Union[str, list] = None, column_selection: list[str] = None, multi_row_data=None,
                    *args, **kwargs) -> None:
        # File path can be given at initialization
        if self.file_path is not None:
            with open(self.file_path, 'r') as file:
                json_data = json.load(file)
        # Otherwise, data can be given as a parameter (string or list
        elif data is not None:
            if isinstance(data, str):
                json_data = json.loads(data)
            elif isinstance(data, list):
                json_data = data
            else:
                raise DataImportError('Data must be a string or list')
        else:
            raise DataImportError('import_data() requires either a file path given at initialization'
                                  ' or data passed into the method.')
        self.imported_data = utils.json_utils.expand_and_normalize(json_data, column_selection, multi_row_data)


# This class desperately needs to be refactored into a json class and a separate API class.
class APISource(DataSource):
    # Not inherited from JsonSource as I think it violates liskov substitution principle.
    """
    Base class for API sources.
    """
    def __init__(self, link: str, params: dict = None, headers: dict = None, json_selection: tuple = tuple()):
        """
        :param link: The link to the API. Additional parts of the link can be added at import time.
        Eg. link = 'api.site.app/' can later have 'extension' added to it.
        :param params: The parameters to be passed to the API. Additional parameters can be added at import time.
        :param headers: The headers to be passed to the API. Additional headers can be added at import time.
        :param json_selection: The keys of the JSON data to be used. Additional keys can be added at export time.
        Eg. json_selection = (0, 'page1', 'data') will select the data [{'name': 'Tom}, {'name': 'Jack'}] from
        the JSON data from [{'page1': {'data': [{'name': 'Tom}, {'name': 'Jack'}]}, timestamp: '1970-01-01'}]
        """
        super().__init__()
        self.link = link
        self.params = {} if params is None else params
        self.headers = {} if headers is None else headers
        self.json_selection = json_selection
        self.json_data_source: JsonSource = JsonSource()

    def import_data(self, link_extension: str = '', column_selection=None, multi_row_data=None, params=None,
                    headers=None) -> None:
        """
        :param link_extension: The link extension to be added to the link.
        :param column_selection: The keys of the JSON data to be used.
        :param multi_row_data: The attributes that you'd like to create new rows for when there's multiple entries in
        the sub-array
        :param params: Additional parameters to be passed to the API.
        :param headers: Additional headers to be passed to the API.
        :return: None
        """
        if params is not None:
            self.params = self.params | params  # Merge preexisting params with new ones

        # Default value for mutable arguments
        column_selection = [] if column_selection is None else column_selection

        self.link += link_extension
        response = requests.get(self.link, params=self.params, headers=self.headers)

        self.json_data_source.import_data(data=response.text,
                                          column_selection=column_selection,
                                          multi_row_data=multi_row_data)

        self.imported_data = self.json_data_source.imported_data


class CsvSource(DataSource):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def import_data(self, *args, **kwargs) -> None:
        self.exported_data = pd.read_csv(self.path, *args, **kwargs)


class ExcelSource(DataSource):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def import_data(self, *args, **kwargs) -> None:
        self.exported_data = pd.read_excel(self.path, *args, **kwargs)


class MultiSource:
    def __init__(self):
        self.data_sources = {}

    def add_data(self, data_name, data_source: DataSource):
        data_source.import_data()
        self.data_sources[data_name] = data_source.export_data()


class DataImportError(Exception):
    pass


