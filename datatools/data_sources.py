import pandas as pd
from abc import ABC, abstractmethod
import requests
import json
from datatools import utils
from datatools import uploading
from datatools import transforming
from typing import Union


class DataSource(ABC):
    """
    Abstract class for importing, transforming, and exporting data from and too various sources processes.
    """
    def __init__(self, transformer: transforming.transformers.Transformer = None,
                 uploader: uploading.uploaders.Uploader = None):
        self.imported_data = None
        self.transformed_data: pd.DataFrame = pd.DataFrame()
        self.exported_data: pd.DataFrame = pd.DataFrame()
        self.failed_exports: pd.DataFrame = pd.DataFrame()
        if transformer is None:
            self.transformer = transforming.transformers.Transformer()
        self.uploader = uploader

    @abstractmethod
    def import_data(self, *args, **kwargs) -> None:
        pass

    def transform_data(self, transformer: transforming.transformers.Transformer = None, *args, **kwargs) -> None:
        if transformer is not None:
            self.transformer = transformer
        self.transformed_data = self.transformer.transform(self.imported_data)

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
    def __init__(self, file_path: str = None, column_selection: list[str] = None, multi_row_data=None):
        super().__init__()
        self.file_path = file_path
        self.column_selection = column_selection
        self.multi_row_data = multi_row_data

    def import_data(self, data: Union[str, list] = None, column_selection: list[str] = None, multi_row_data=None,
                    *args, **kwargs) -> None:
        # Provided at initialization or as a parameter
        if column_selection is not None:
            self.column_selection = column_selection
        # Provided at initialization or as a parameter
        if multi_row_data is not None:
            self.multi_row_data = multi_row_data
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
        self.imported_data = utils.json_utils.expand_and_normalize(json_data, self.column_selection, self.multi_row_data)


class APISource(DataSource):
    # Not inherited from JsonSource as I think it violates liskov substitution principle.
    """
    Base class for API sources.
    """
    def __init__(self, link: str, params: dict = None, headers: dict = None, json_selection: tuple = tuple(),
                 column_selection=None, multi_row_data=None):
        """
        :param link: The link to the API. Additional parts of the link can be added at import time.
        Eg. link = 'api.site.app/' can later have 'extension' added to it.
        :param params: The parameters to be passed to the API. Additional parameters can be added at import time.
        :param headers: The headers to be passed to the API. Additional headers can be added at import time.
        :param json_selection: The keys of the JSON data to be used. Additional keys can be added at export time.
        Eg. json_selection = (0, 'page1', 'data') will select the data [{'name': 'Tom}, {'name': 'Jack'}] from
        the JSON data from [{'page1': {'data': [{'name': 'Tom}, {'name': 'Jack'}]}, timestamp: '1970-01-01'}]
        :param column_selection: The dataframe columns to be used after import the transformed data. Hierarchical
        keys are separated by '.'.
        :param multi_row_data: The attributes that you'd like to create new rows for when there's multiple entries in
        the sub-array (just give the lowest attribute in the key hierarchy).
        """
        super().__init__()
        self.link = link
        self.params = {} if params is None else params
        self.headers = {} if headers is None else headers
        self.json_selection = json_selection
        self.column_selection = [] if column_selection is None else column_selection
        self.multi_row_data = [] if multi_row_data is None else multi_row_data
        self.json_data_source: JsonSource = JsonSource()

    def _update_response_inputs(self, link_extension: str, params: dict = None, headers: dict = None) -> None:
        """
        Updates the link, params and headers of the response.
        :param link_extension: The extension to be added to the link.
        :param params: The parameters to be added to the link.
        :param headers: The headers to be added to the link.
        """
        self.link += link_extension
        if params is not None:
            self.params.update(params)
        if headers is not None:
            self.headers.update(headers)

    def _update_data_manipulation(self, column_selection, multi_row_data):
        """
        Updates the column_selection and multi_row_data of the data manipulation of the json data. These
        are later passed to utils.json_utils.expand_and_normalize
        :param column_selection:
        :param multi_row_data:
        :return:
        """
        self.column_selection += [] if column_selection is None else column_selection
        self.multi_row_data += [] if multi_row_data is None else multi_row_data

    def _get_response(self):
        return requests.get(self.link, params=self.params, headers=self.headers)

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
        self._update_data_manipulation(column_selection, multi_row_data)
        self._update_response_inputs(link_extension, params, headers)
        response = self._get_response()

        self.json_data_source.import_data(data=response.text,
                                          column_selection=column_selection,
                                          multi_row_data=multi_row_data)

        self.imported_data = self.json_data_source.imported_data


class CsvSource(DataSource):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def import_data(self, *args, **kwargs) -> None:
        self.imported_data = pd.read_csv(self.path, *args, **kwargs)


class ExcelSource(DataSource):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def import_data(self, *args, **kwargs) -> None:
        self.imported_data = pd.read_excel(self.path, *args, **kwargs)


class MultiSource:  # Maybe this should be a datasource with a concatenation function passed at initialization?
    def __init__(self):
        self.data_sources = {}

    def add_data(self, data_name, data_source: DataSource):
        data_source.import_data()
        self.data_sources[data_name] = data_source.export_data()

    def concat(self):
        """
        Concatenates all the data sources into one dataframe with the datasource name as an additional column.
        :return:
        """
        self.add_data_names()
        return pd.concat(self.data_sources.values(), ignore_index=True, axis='rows')

    def add_data_names(self):
        """
        Adds the datasource name as a new column to each of the dataframes
        :return:
        """
        for data_source_name, data_source in self.data_sources.items():
            data_source['datasource'] = data_source_name


class DataImportError(Exception):
    pass


