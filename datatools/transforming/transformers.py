from typing import Callable
from dataclasses import dataclass
import pandas as pd
import functools


@dataclass
class PipelineStep:
    func: Callable
    args: tuple = tuple()
    """
    A pipeline step is a function that takes only a dataframe returns only a dataframe. Additional arguments can be
    provided at the PipelineStep initialization along with the function provided. This can assist with "customizing'
    the provided function for the specific use case.
    :param func: The function to be called.
    :param args: Additional arguments to be passed to the function.
    """
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.func(df, *self.args)


class Transformer:
    def __init__(self, *pipeline_functions: Callable):
        self._functions = None
        self.pipeline = None
        self.set_functions(list(pipeline_functions))

    def get_functions(self):
        return self._functions

    def set_functions(self, functions):
        self._functions = functions
        self._update_pipeline()

    def add_function(self, function, index=None):
        if index is None:
            self._functions.append(function)
        else:
            self._functions.insert(index, function)
        self._update_pipeline()

    def remove_function(self, index):
        del self._functions[index]
        self._update_pipeline()

    def _update_pipeline(self):
        if not self._functions:  # Checks if empty
            self.pipeline = lambda x: x
        else:
            self.pipeline = functools.reduce(lambda f, g: lambda x: f(g(x)), self._functions)

    def transform(self, df):
        return self.pipeline(df)
