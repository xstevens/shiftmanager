#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for AdminMixin functions

Test Runner: PyTest
"""

import datetime


def test_random_password(shift):
    for password in [shift.random_password() for i in range(0, 6, 1)]:
        assert len(password) < 65
        assert len(password) > 7
        for char in r'''\/'"@ ''':
            assert char not in password


def test_create_user(shift):
    batch = shift.create_user("swiper", "swiperpass",
                              groups=['analyticsusers'],
                              wlm_query_slot_count=2)
    assert batch == (
        "CREATE USER swiper IN GROUP analyticsusers PASSWORD 'swiperpass';\n"
        "ALTER USER swiper SET wlm_query_slot_count = 2"
    )

    batch = shift.create_user("swiper", "swiperpass",
                              valid_until=datetime.datetime(2015, 1, 1))
    assert batch == (
        "CREATE USER swiper PASSWORD 'swiperpass' "
        "VALID UNTIL '2015-01-01 00:00:00'"
    )


def test_alter_user(shift):
    statement = shift.alter_user("swiper", password="swiperpass")
    assert statement == "ALTER USER swiper PASSWORD 'swiperpass'"
