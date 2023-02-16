from __future__ import annotations  # This makes type hints considered as strings to avoid import loops


from . import sourcing
from . import transforming
from . import uploading

import pandas as pd


class DataHandler:
    def __init__(self, source: sourcing.DataSource = None,
                 transformer: transforming.Transformer = None,
                 uploader: uploading.Uploader = None):
        self.source = source
        if transformer is None:
            self.transformer = transforming.Transformer()
        self.transformer = transformer
        self.uploader = uploader

        self.imported_data: pd.DataFrame = pd.DataFrame()
        self.transformed_data: pd.DataFrame = pd.DataFrame()
        self.exported_data: pd.DataFrame = pd.DataFrame()

        self.failed_exports = pd.DataFrame()

    def import_data(self, *args, **kwargs) -> None:
        self.source.import_data(*args, **kwargs)
        self.imported_data = self.source.import_data()

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


