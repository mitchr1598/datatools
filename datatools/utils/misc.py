def list_difference(full, exclude):
    ex_set = set(exclude)
    return [el for el in full if el  not in ex_set]