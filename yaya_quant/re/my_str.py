import os,re

# extract_number from string
re_digits = re.compile(r'(\d+)')
def extract_number(str):
    pieces = re_digits.split(str)
    return int(pieces[1])


# 检查字符串是否满足正则表达式
def ifmatch(re_exp, str):
    res = re.search(re_exp, str)
    if res:
        return True
    else:
        return False

# 输出txt 同花顺可读
def write_code(code_list):
    filename = 'code_list.txt'
    with open(filename, 'w') as f:
        for code in code_list:
            f.write(code)
            f.write('\n')




