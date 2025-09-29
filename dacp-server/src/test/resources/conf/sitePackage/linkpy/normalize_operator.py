# file: operators/normalize_operator.py

def process(rows):
    """
    :param rows: iterable of rows (each row is a list of values)
    :return: generator of processed rows
    """
    for row in rows:
        total = sum(x for x in row if isinstance(x, (int, float)))
        if total == 0:
            yield row  # avoid division by zero
        else:
            yield [x / total if isinstance(x, (int, float)) else x for x in row]
