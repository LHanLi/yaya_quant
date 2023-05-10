import os,re

# extract_number from string
re_digits = re.compile(r'(\d+)')
def extract_number(str):
    pieces = re_digits.split(str)
    return int(pieces[1])
