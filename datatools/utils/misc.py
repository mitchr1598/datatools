from typing import Iterable


def list_difference(full, exclude):
    ex_set = set(exclude)
    return [el for el in full if el not in ex_set]


def first_matching(search_space: Iterable, search_keys: Iterable) -> list:
    """
    Searches through an iterable of iterables and returns the first element of each
    sub-iterable that matches one of the search keys.

    eg. first_matching([(1,2,3), (4,5,6)], {1,3,5}) returns [1, 5]
    """
    result = []
    for sub_iter in search_space:
        match_found = False
        for item in sub_iter:
            if item in search_keys:
                result.append(item)
                match_found = True
                break
        if not match_found:
            result.append(sub_iter[0])
    return result
