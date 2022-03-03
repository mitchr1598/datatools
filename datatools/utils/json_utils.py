import pandas as pd
from typing import Union


def expand_lists(json_array: list, multi_data_att: list = None):
    """
    Given a list of json objects, remove the lists of each object by:
        - Creating new object for attributes that demand multiple rows via multi_data_att
        - Switch the single element lists to dictionaries
        - Turn the remaining lists into empty dictionaries
    :param json_array: An array of json objects that you don't want sub arrays for
    :param multi_data_att: The object attributes that you'd like to create new objects for to retain all rows
    :return:
    """
    new_objects = []  # Master list of new objects, used when some attributes have multiple pieces of data
    if multi_data_att is None:  # Default value for mutable object
        multi_data_att = []
    for i, data_el in enumerate(json_array):  # For each "object" (dict) in "array" (list)
        for key, value in data_el.items():  # For each attribute in "object" (dict)
            if isinstance(value, list):  # If it's another "array" (list)
                # Keep if there's only one, otherwise kill it
                if len(json_array[i][key]) == 0:
                    json_array[i][key] = {}
                elif len(json_array[i][key]) == 1:
                    json_array[i][key] = json_array[i][key][0]
                else:
                    if key in multi_data_att:  # Specifying the columns to make multiple rows for
                        for sub_i, obj in enumerate(value):  # Iterate through each piece of data
                            if sub_i == 0:
                                continue
                            new_row = data_el.copy()  # Construct a new_row, from the current one
                            new_row[key] = obj  # Make this new row a single piece of data by setting the value at the key
                            new_objects.append(new_row)  # Append to the master list of new rows (gets added at the end)
                        json_array[i][key] = json_array[i][key][0]
                    else:
                        json_array[i][key] = {}
    json_array.extend(new_objects)
    return json_array


def expand_and_normalize(data, column_filter, multi_row_data=None):
    """
    Given a list of json objects, turn the list into a dataframe
    :param data: json_data to turn into a dataframe
    :param column_filter: The resulting columns you'd like. Separate hierarchical attributes with a '.'
    :param multi_row_data: The attributes that you'd like to create new rows for when there's multiple entries in
    the sub-array (just give the lowest attribute)
    :return:
    """
    if multi_row_data is None:  # Default value for mutable object
        multi_row_data = []
    json_keys = {column.split('.')[0] for column in column_filter}  # Get the top level keys
    filtered_data = [{key: row[key] for key in json_keys if key in row} for row in data]
    expanding_data = expand_lists(filtered_data, multi_data_att=multi_row_data)
    normalized_df = pd.json_normalize(expanding_data)
    existing_columns = [col for col in column_filter if col in normalized_df.columns]
    return normalized_df[existing_columns]


def json_indexer(json_data: Union[list, dict], index: tuple):
    """
    Given a json object/array and a tuple of keys, return the value of the key
    :param json_data: The json object/array to index
    :param index: The tuple of keys to index
    :return: The value of the json_data at the specified key
    """
    for key in index:
        json_data = json_data[key]
    return json_data
