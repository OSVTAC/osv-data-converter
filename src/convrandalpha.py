#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Script to strip spaces from a copy and paste randomized alphabet
"""

import re

a = """
    A
    Z
    R
    C
    H
    E
    W

    L
    F
    N
    Y
    X
    O
    D

    K
    S
    M
    J
    P
    Q
    V

    U
    B
    G
    I
    T
"""
a_seq = re.sub(r'\s+','', a)
a_inv = [' ']*26
for i, l in enumerate(a_seq):
    a_inv[ord(l)-ord('A')] = chr(i+ord('A'))
print(a_seq)
print(''.join(a_inv))