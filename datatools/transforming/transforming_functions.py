import itertools

import pandas as pd
from pandas.api.types import is_float
from typing import Union, Iterable
import datatools.utils as utils
import warnings


def remove_top_n(df, n: int):
    """
    Removes the top n rows from the dataframe
    """
    return df.iloc[n:]


def remove_rows_on_value(df, columns: Union[list, set], values: Union[str, list, set]):
    """
    Removes rows that contain string value(s) passed into function
    """
    # creates a set out of columns passed into function
    # faster searching in sets vs lists or dict
    values_set = set(values)
    for column_header in columns:
        # strips cells in dataframe that contain values in values_set
        # then create a dataframe without the rows containing the stripped cells
        df = df[~ df[column_header].apply(str).str.strip().isin(values_set)]
    return df


def promote_nth_row_to_headers(df, n):
    """
    Discards all rows above n and promotes row n to the dataframe headers
    """
    new_header = df.iloc[n].fillna('col')  # grab the nth row for the header
    df = df[n + 1:]  # take the data less the header row and above the header row
    df.columns = new_header  # set the header row as the df header
    return df


def search_in_rows(df, search_values: Union[list, set]):
    """
    Finds the index where search_values if first a subset of the row
    """
    # creates a set out of columns passed into function
    # faster searching in sets vs lists or dict
    search_values_set = set(search_values)
    # finds row index of all search_values matches in df and returns the highest index
    is_subset = df.apply(lambda x: search_values_set.issubset(x), axis='columns', raw=True)
    return is_subset.idxmax() if is_subset.any() else None


def search_to_promote(df, search_values: Union[list, set], check_header=False):
    """
    Will search for headers to promote. All search values must be found in the row to be promoted
    """
    required_values = set(search_values)
    if check_header and required_values <= set(df.columns):  # Check if already in header
        return df
    i = search_in_rows(df, search_values)
    return df if i is None else promote_nth_row_to_headers(df, i)


def search_to_promote_multi(df, search_values_multiple: tuple[list, tuple, set], check_header=False):
    """
    Will search for headers to promote, with various options to match (will promote the earliest found).
    Each element of the tuple search_values_multiple must be a list, tuple, or set of values to search for
    """
    for search_values in search_values_multiple:
        required_values = set(search_values)
        new_df = search_to_promote(df, required_values, check_header=check_header)
        if not df.equals(new_df) or required_values <= set(df.columns):
            return new_df
    return df


def find_headers_to_promote(df, search_values_multiple: Union[list[Iterable], tuple[Iterable]], check_header=False):
    """
    Promotes multiple rows to headers if they mat.
    :param df: The dataframe to search

    :param search_values_multiple: For a row to be promoted, it must contain at least one sub-element from every element
    in search_values_multiple.

    For example search_values_multiple = [('AA', 'aa', 'Aa', 'aA'), ('BB', 'bb', 'Bb', 'bB')]
    will promote a row that contain one element with "double A" AND one element with "double B"
    ['aA', 'cc'] will not be promoted because it does not contain any of the elements in the second
        tuple of search_values_multiple
    ['Bb', 'cc'] will not be promoted because it does not contain any of the elements in the first
        tuple of search_values_multiple
    ['aA', 'Bb'] will be promoted because it contains at least one element from each tuple

    :param check_header:
    :return:
    """
    # TODO: Deal with empty search
    # TODO: Deal with multiple search values - at the moment it needs exact matches from all values
    # TODO: Need to deal with incorrectly passed data in search_values_multiple
    # TODO: This probably needs regex compatibility 🤮

    # Identifies all rows that contain the search values, then creates the column headers
    # from the rows that contain the search values
    is_header_row = df.apply(lambda row: contains_multiple_of(row, search_values_multiple), axis='columns', raw=True)
    rows_with_headers = df[is_header_row].values.tolist()
    if check_header and contains_multiple_of(df.columns, search_values_multiple):
        rows_with_headers.insert(0, list(df.columns))
    # create a multi index out of the rows that contain the search values
    # and the column headers
    df.columns = pd.MultiIndex.from_tuples(list(zip(*rows_with_headers)))

    all_headers = set(itertools.chain.from_iterable(search_values_multiple))  # One big set of all searching headers
    df.columns = utils.misc.first_matching(df.columns, all_headers)  # Set the column headers to the first matching header
    return df


def contains_one_of(iterable, search_values: Iterable):  # TODO: Move to utils
    """
    Returns true if the iterable contains one of the search values
    """
    iterable = set(iterable)
    return any([value in iterable for value in search_values])


def contains_multiple_of(iterable, search_values_multiple: Union[list[Iterable], tuple[Iterable]]):  # TODO: Move to utils
    """
    Returns true if the series contains one value from each element in search_values_multiple
    """
    return all([contains_one_of(iterable, search_values) for search_values in search_values_multiple])


def remove_n_column(df, n: int):
    """
    Removes column n from dataframe
    """
    # drops column by int index
    return df.drop(df.columns[n], axis=1)


def remove_columns(df, columns: list[Union[str, int]]):
    """
    Removes columns from dataframe by either position or name (but not both)
    """
    existing_columns = set(df.columns)
    if isinstance(columns[0], str):
        return df.drop([col for col in columns if col in existing_columns], axis=1)
    elif isinstance(columns[0], int):
        return df.drop([df.columns[n] for n in columns], axis=1)


def remove_float_columns(df, col_subset: Union[str, list[Union[int, str]]] = 'all',
                         required_hits: Union[float, int] = 0.5):
    """
    Removes float columns. Optionally only look at certain columns (given by name or position). required_hits is the proportion of values that must be a float
    """
    # checks if all columns are specified
    if col_subset == 'all':
        col_subset = df.columns
    # otherwise, checks if the first element in the column subset is an int
    elif isinstance(col_subset[0], int):
        col_subset = [df.columns[i] for i in col_subset]
    # if required hits is a proportion, calculate the number of hits required relative to the number of rows
    if required_hits < 1 and required_hits > 0:
        required_hits = required_hits * df.shape[0]
    # iterates through each specified row and counts the number of floats in the column
    for col in col_subset:
        vals = df[col].values
        hits = [is_float(el) for el in vals]
        # if the number of floats in the column is greater than the required hits, remove the column from the dataframe
        if sum(hits) > required_hits:
            df.drop(col, axis='columns', inplace=True)
    return df


def add_column(df, column, name='col'):
    """
    Adds a new column to dataframe containing rows passed into function
    If a name for the column is not given, it defaults to 'col'
    """
    df[name] = column
    return df


def add_column_if_dne(df, column, name='col'):
    """
    If column name is not already a column, adds a new column to dataframe containing rows passed into function
    If a name for the column is not given, it defaults to 'col'
    """
    if name not in df:
        df[name] = column
    return df


def select_columns(df, columns: Union[list, set], on_error='raise'):
    """
    Returns a dataframe containing the defined columns
    :param df: The dataframe to select columns from
    :param columns: The columns to select
    :param on_error: What to do if a column is not found. Options are 'ignore', 'raise', 'warn'
    :return:
    """
    if on_error not in {'ignore', 'raise', 'warn'}:
        raise ValueError(f"on_error must be one of 'ignore', 'raise', 'warn'")
    if on_error == 'raise':
        for col in columns:
            if col not in df.columns:
                raise ValueError(f'Column {col} not found in dataframe with columns {list(df.columns)}')
    elif on_error == 'warn':
        for col in columns:
            if col not in df.columns:
                warnings.warn(f'Column {col} not found in dataframe with columns {list(df.columns)}')
    columns = [col for col in columns if col in df.columns]
    return df[columns]


def select_columns_range(df, start: int, end: int):
    """
    Returns a dataframe containing the defined columns
    """
    df = df.iloc[:, start: end]
    return df


def rename_nth_column(df, n: int, new_name: str):
    """"
    Renames the nth column
    """
    # creates a copy of the dataframe
    df_copy = df.copy()
    # creates a list of the column names in the dataframe
    cols = list(df_copy.columns)
    # changes column name element in list at the specified index
    cols[n] = new_name
    # sets column names of the dataframe copy to the list of column names
    df_copy.columns = cols
    return df_copy


def replace_column_names(df, names: Union[list, set]):
    """
    Renames all column names from a list of names
    """
    df.columns = names
    return df


def rename_like(df, rename_regex, new_name: str):
    """
    Takes in a regex and renames all matching columns
    """
    matching_columns = df.filter(regex=rename_regex).columns
    n_mat = len(matching_columns)
    if n_mat == 0:
        return df
    else:
        name_mapper = {col: new_name for col in matching_columns}
        return df.rename(columns=name_mapper)


def headers_string_and_strip(df):
    """
    Removes any leading and trailing spaces in df columns
    """
    df.columns = [str(col).strip() for col in df.columns]
    return df


def dedupe_column_names(df, keep_first=False):
    """
    If not keep_first, then adds a number to the end of duplicate column names based on their order of appearance
    in the dataframe
    """
    # TODO: Run a test on this
    if keep_first:
        # df.columns.duplicated(keep='first') returns false if a column is unique up to that point.
        return df.loc[:, ~df.columns.duplicated(keep='first')]

    cols = pd.Series(df.columns)
    for dup in df.columns[df.columns.duplicated(keep=False)]:  # Get's all duplicated columns
        # TODO: I think this is doubling up on the duplicates, essentially redoing work.
        #  Co-pilot was making solid suggestions
        new_cols = []
        for d_idx in range(df.columns.get_loc(dup).sum()):
            new_cols.append(dup + '.' + str(d_idx))
        cols[df.columns.get_loc(dup)] = new_cols
    df.columns = cols
    return df


def drop_duplicate_columns(df, subset: list[Union[int, str]]):
    """
    Drops columns with duplicate names, keeping the first instance
    :param subset: The subset of columns to consider, optionally set using index or column name
    """
    keep_cols = ~df.columns.duplicated()
    # df.columns.duplicated() returns a boolean array. If it is False then the column name is unique up to that point,
    # if it is True then the column name is duplicated earlier.
    # For example, using ['alpha','beta','alpha'], the returned value would be [False,False,True].
    # The ~ operator flips the boolean array, so the returned value would be [True,True,False]

    # Now change the keep_cols to only include the subset given
    if isinstance(subset[0], int):
        subset = set(subset)  # Converted to set for faster contains check
        # Flips the boolean value if it's not in the subset (ie. keep it)
        # Subset of columns specified by index
        keep_cols = [is_dupe if i in subset else True
                     for i, is_dupe in enumerate(keep_cols)]  # list comp wrapper to filter for subset
    else:
        subset = set(subset)  # Converted to set for faster contains check
        # Flips the boolean value if it's not in the subset (ie. keep it)
        # Subset of columns specified by name
        keep_cols = [is_dupe if col_name in subset else True for col_name, is_dupe in zip(df.columns, keep_cols)]

    df = df.loc[:, keep_cols]
    return df


def drop_duplicate_rows(df, cols: list, keep='first'):
    """
    Removes duplicate rows based on 1 or more columns, keep first or last row
    """
    df = df.drop_duplicates(
        subset=cols,
        keep=keep).reset_index(drop=True)
    return df


def convert_to_datetime(df, cols: Union[list, set]):
    """
    Takes a list of column names and converts any matching columns to datetime format
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def convert_to_numeric(df, cols: Union[list, set]):
    """
    Takes a list of column names and converts any matching columns to numerical format
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def strip_every_str_column(df, exclude=None):
    """
    Takes in a dataframe and a list of columns to exclude.
    For the columns not excluded, removes leading and trailing spaces in columns containing strings.
    """
    df = df.copy()
    df_obj = df.select_dtypes('object').astype(str)
    str_cols = utils.misc.list_difference(df_obj.columns, exclude) if exclude is not None else df_obj.columns
    sc = df_obj[str_cols].apply(lambda x: x.str.strip())
    df[str_cols] = sc
    return df


def split_string_column(df, column: str, char: chr, new_column_name=None):
    """
    Splits a given string column based on a given character and puts splits in new column,
    if multiple matching characters are in a cell, it will create that many extra columns
    """
    if new_column_name is None:
        new_column_name = f"{column}_split_"

    # Splits column rows into a new dataframe
    splits = df[column].str.split(char, expand=True)
    # creates a list of column names based on the sequence of the split
    col_names = [f"{new_column_name}{i}" for i, _ in enumerate(splits.columns)]
    # renames the split columns to the new names
    splits = replace_column_names(splits, col_names)
    # adds the split columns to the end of the original dataframe
    df = df.join(splits)
    return df


def max_between_columns(df, columns: Union[list, set], new_name='max'):
    """
    Adds a column to the df containing the max value for a row between 2 or more columns
    """
    # .max call passes numerical_only as False, this is so an error is raised if a non-numerical column is given
    return pd.concat([df, df[columns].max(axis='columns', numeric_only=False).rename(new_name)], axis='columns')


def min_between_columns(df, columns, new_name='min'):
    """
    Adds a column to the df containing the min value for a row between 2 or more columns
    """
    # .min call passes numerical_only as False, this is so an error is raised if a non-numerical column is given
    return pd.concat([df, df[columns].min(axis='columns', numeric_only=False).rename(new_name)], axis='columns')


def remove_character(df, character: chr, columns=None):
    """
    Takes in a dataframe, character and optionally a list of columns.
    If a list of columns is not given, will use all columns in dataframe.
    Replaces all instances of character in columns with an empty char value.
    """
    if columns is None:
        columns = df.columns
    for col_header in columns:
        df[col_header] = df[col_header].str.replace(character, '')
    return df


def remove_non_digits(df, columns: Union[list, set]):
    """
    Removes non digits, but doesn't turn pure strings into null. Need to run convert_to_numeric to enable that
    """
    for col_header in columns:
        df[col_header] = df[col_header].astype(str).str.extract(r'[-+]?(\d*\.\d+|\d+)')
    return df


def name_preferred_extraction(df):
    """
    Gets the text between the brackets of the column 'GivenName', ie. the preferred name
    """
    df['Preferred'] = df['GivenName'].apply(
        lambda name: name[name.find("(") + 1:name.find(")")] if '(' in name else name)
    return df


def name_given1_extraction(df):
    """
    Gets the text before the brackets of the column 'GivenName', ie. the Given1 name
    """
    df['Given1'] = df['GivenName'].apply(lambda name: name[:name.find("(")] if '(' in name else name)
    return df


def main():
    df = pd.DataFrame([['y4', 'rs', 'st'], [1, 23, 6], ['st', 'straw', 'hgf'], ['stanine', 'RawScore', 'rsdfs'], ['stanine', 'rs', 'rsdfs']])
    print(find_headers_to_promote(df, [{'st', 'stanine'}, {'rs', 'RawScore'}]))


if __name__ == '__main__':
    main()
