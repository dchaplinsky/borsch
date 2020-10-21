def num_strip(val):
    return str(val).strip().replace(" ", "").replace(",", ".").replace("\xa0", "")


def parse_amount(val):
    return float(num_strip(val))


def parse_int(val):
    return int(num_strip(val))

