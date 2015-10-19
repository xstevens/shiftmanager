#!/usr/bin/env python

from functools import wraps
import math


def memoize(f):
    """
    Memoization decorator for single argument methods.
    """
    memo = {}

    @wraps(f)
    def wrapper(self, key):
        val = memo.get(key)
        if not val:
            val = memo[key] = f(self, key)
        return val

    return wrapper


def recur_dict(accum, value, parent=None, list_idx=None):
    """
    Recurse through the dict `value` and update `accum` for new fields.
    `list_idx` will indicate the idx of a list

    Parameters
    ----------
    accum : set
        Accumulator
    value : dict
        Current value to parse
    parent : string, default None
        Parent key to get key nesting depth.
    list_idx : int
        List index to specify list location

    Example
    -------
    >>> r = recur_dict(set(), {"one": 1, "two": {"three": [1, 2, 3]}},
    ...                list_idx=0)
    >>> sorted(r)
    ["$['one']", "$['two']['three'][0]"]
    """
    list_idx = list_idx or 0
    parent = parent or '$'

    if isinstance(value, dict):
        for k, v in value.items():
            fmt_k = "['{}']".format(k)
            parent_path = ''.join([parent, fmt_k]) if parent != '' else fmt_k
            if isinstance(v, (list, dict)):
                recur_dict(accum, v, parent_path, list_idx=list_idx)
            else:
                accum.add(parent_path)

    elif isinstance(value, list):
        accum.add(''.join([parent, "[{}]".format(str(list_idx))]))
        return accum

    return accum


def linspace(start, stop, num):
    """Quick linspace-ish integer generator for chunking"""
    step = (stop - start)/float(num)
    res = [start]
    accum = start
    for i in range(1, num, 1):
        accum = accum + step
        if accum > stop:
            break
        res.append(int(math.floor(accum)))
    return res
