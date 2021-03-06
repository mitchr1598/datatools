from typing import Callable
from dataclasses import dataclass, field
import pandas as pd
import functools


class PipelineStep:
    """
    A pipeline step is a function that takes only a dataframe returns only a dataframe. Additional arguments can be
    provided at the PipelineStep initialization along with the function provided. This can assist with "customizing'
    the provided function for the specific use case.
    :param func: The function to be called.
    :param args: Additional arguments to be passed to the function.
    """
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.func(df, *self.args, **self.kwargs)


class Transformer:
    def __init__(self, *pipeline_functions: Callable):
        self._functions = None
        self.pipeline = None
        self.set_functions(list(pipeline_functions))

    def get_functions(self):
        return self._functions

    def set_functions(self, functions):
        self._functions = functions

    def add_function(self, function, index=None):
        if index is None:
            self._functions.append(function)
        else:
            self._functions.insert(index, function)

    def remove_function(self, index):
        del self._functions[index]

    def transform(self, df):
        if not self._functions:  # Checks if empty
            return df
        else:
            return functools.reduce(lambda f, g: lambda x: g(f(x)), self._functions)(df)
