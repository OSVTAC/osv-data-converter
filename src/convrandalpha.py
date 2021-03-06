#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Script to strip spaces from a copy and paste randomized alphabet
"""

import re

a = """


    B
    A
    E
    Z
    T
    L
    G

    H
    P
    O
    S
    N
    M
    D

    Q
    F
    I
    C
    V
    U
    X

    Y
    K
    W
    R
    J

"""
a_seq = re.sub(r'\s+','', a)
a_inv = [' ']*26
for i, l in enumerate(a_seq):
    a_inv[ord(l)-ord('A')] = chr(i+ord('A'))
print(a_seq)
print(''.join(a_inv))