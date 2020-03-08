#!/usr/bin/env python
# coding: utf-8
def find_locate_max(lst):                   # найти наибольший элемент в списке и его позицию (-ии)
    biggest = max(lst)
    return biggest, [index for index, element in enumerate(lst) if biggest == element]