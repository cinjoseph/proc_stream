# -*- coding:utf-8 -*-

import importlib


def level_print(l, string):
    string = str(string)
    tmp = ''
    for i in range(l):
        tmp += '\t'
    string = string.split('\n')
    for i in range(len(string)):
        string[i] = tmp + string[i]
    string = '\n'.join(string)
    print(string)


def array_printable(l):
    if type(l) != list:
        return str(l)
    new_l = []
    # l = copy.deepcopy(l)
    for _l in l:
        if type(_l) == list:
            new = array_printable(_l)
            new_l.append(_l)
        else:
            new_l.append(str(_l))
    return new_l
