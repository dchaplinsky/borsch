from decimal import Decimal


def num_strip(val):
    return str(val).strip().replace(" ", "").replace(",", ".")


def parse_amount(val):
    return Decimal(num_strip(val))


def parse_int(val):
    return int(num_strip(val))

