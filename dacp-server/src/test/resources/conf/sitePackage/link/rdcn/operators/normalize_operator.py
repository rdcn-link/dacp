# file: operators/normalize_operator.py

def process(rows):
    return [
        row if (total := sum(x for x in row if isinstance(x, (int, float)))) == 0
        else [round(x / total, 2) if isinstance(x, (int, float)) else x for x in row]
        for row in rows
    ]
