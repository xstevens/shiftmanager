#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Util tests
"""

import shiftmanager.util as util


def test_recur_dict():
    test_1 = {"one": 1}
    assert util.recur_dict(set(), test_1) == set(["$['one']"])

    test_2 = {"one": 1, "two": {"three": 1}}
    res_2 = set(["$['one']", "$['two']['three']"])
    assert util.recur_dict(set(), test_2) == res_2

    test_3 = {"one": 1, "two": {"three": {"four": 4}, "five": [1, 2, 3]}}
    res_3 = set(["$['one']", "$['two']['three']['four']",
                 "$['two']['five'][0]"])
    assert util.recur_dict(set(), test_3, list_idx=0) == res_3

    test_4 = {"one": [1, 2]}
    assert util.recur_dict(set(), test_4, list_idx=1) == set(["$['one'][1]"])
