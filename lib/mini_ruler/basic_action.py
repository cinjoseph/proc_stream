# -*- coding:utf-8 -*-

import re
import ipaddr


def ip_in_net(ip, ip_range):
    net = ipaddr.IPv4Network(ip_range)
    ip = ipaddr.IPv4Address(ip)
    if ip in net:
        return True
    return False

def re_match(pattern, s):
    if type(s) != str:
        return False
    if type(pattern) != str:
        return False

    if re.match(pattern, s):
        return True
    return False


def num_in_range(x, low, high):
    if high >= x >= low:
        return True
    return False


def exist(p):
    if not p:
        return False
    return True
