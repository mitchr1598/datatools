import pandas as pd
from abc import ABC, abstractmethod
import datatools.etl
import requests
import json
import datatools.utils


class DataSource(ABC):
    """
    Abstract class for ETL processes.
    """
    def __init__(self):
        self.imported_data: pd.DataFrame = pd.DataFrame()
        self.transformed_data: pd.DataFrame = pd.DataFrame()
        self.exported_data: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def import_data(self, *args, **kwargs) -> None:
        pass

    def transform_data(self, transformer: datatools.transformers.Transformer) -> None:
        self.transformed_data = transformer.transform(self.imported_data)

    @abstractmethod
    def export_data(self, *args, **kwargs) -> pd.DataFrame:
        pass


class DataframeSource(DataSource):
    def __init__(self):
        super().__init__()

    def import_data(self, df) -> None:
        self.imported_data = df

    def export_data(self) -> pd.DataFrame:
        self.exported_data = self.transformed_data if self.transformed_data is not None else self.imported_data
        return self.exported_data


class APISource(DataSource):  # Probably need to create a bridge class.
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
        self.link = link
        self.params = {} if params is None else params
        self.headers = {} if headers is None else headers
        self.json_selection = json_selection
        self.json_data = None

    def import_data(self, link_extension: str = '', params=None, headers=None) -> None:
        """
        :param link_extension: The link extension to be added to the link.
        :param params: Additional parameters to be passed to the API.
        :param headers: Additional headers to be passed to the API.
        :return: None
        """
        if params is not None:
            self.params = self.params | params  # Merge preexisting params with new ones
        self.link += link_extension
        response = requests.get(self.link, params=self.params, headers=self.headers)
        self.json_data = json.loads(response.text)

    def export_data(self, column_selection: list[str], json_selection: tuple = tuple(), multi_row_data=None,
                    *args, **kwargs) -> pd.DataFrame:
        """
        :param column_selection: The resulting columns you'd like. Separate hierarchical attributes with a '.'
        :param json_selection: Additional keys to be selected from the JSON data.
        :param multi_row_data: The attributes that you'd like to create new rows for when there's multiple entries in
        the sub-array (just give the lowest attribute)
        :return:
        """
        self.json_selection += json_selection
        data = datatools.utils.json_utils.json_indexer(self.json_data, self.json_selection)
        df = datatools.utils.json_utils.expand_and_normalize(data, column_selection, multi_row_data=multi_row_data)
        return df


class CsvSource(DataSource):
    def __init__(self, path):
        self.path = path
        self.df = None

    def import_data(self) -> None:
        self.df = pd.read_csv(self.path)

    def export_data(self) -> pd.DataFrame:
        return self.df


class MultiSource:
    def __init__(self):
        self.data_sources = {}

    def add_data(self, data_name, data_source: DataSource):
        data_source.import_data()
        self.data_sources[data_name] = data_source.export_data()
