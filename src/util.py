# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Miscellaneous utilities."""

import json
import math

def unique(array):
    """Return a sorted array of the unique elements in `array`.

    No element may be a floating-point NaN.  If your data set includes
    NaNs, omit them before passing them here.
    """
    for x in array:
        assert not (isinstance(x, float) and math.isnan(x))
    if len(array) < 2:
        return array
    array_sorted = sorted(array)
    array_unique = [array_sorted[0]]
    for x in array_sorted[1:]:
        assert array_unique[-1] <= x
        if array_unique[-1] != x:
            array_unique.append(x)
    return array_unique

def unique_indices(array):
    """Return an array of the indices of the unique elements in `array`.

    No element may be a floating-point NaN.  If your data set includes
    NaNs, omit them before passing them here.
    """
    for x in array:
        assert not (isinstance(x, float) and math.isnan(x))
    if len(array) == 0:
        return []
    if len(array) == 1:
        return [0]
    array_sorted = sorted((x, i) for i, x in enumerate(array))
    array_unique = [array_sorted[0][1]]
    for x, i in array_sorted[1:]:
        assert array[array_unique[-1]] <= x
        if array[array_unique[-1]] != x:
            array_unique.append(i)
    return sorted(array_unique)

def float_sum(iterable):
    """Return the sum of elements of `iterable` in floating-point.

    This implementation uses Kahan-Babuška summation.
    """
    s = 0.0
    c = 0.0
    for x in iterable:
        xf = float(x)
        s1 = s + xf
        if abs(x) < abs(s):
            c += ((s - s1) + xf)
        else:
            c += ((xf - s1) + s)
        s = s1
    return s + c

def casefold(string):
    # XXX Not really right, but it'll do for now.
    return string.upper().lower()

def cursor_row(cursor, nullok=None):
    if nullok is None:
        nullok = False
    try:
        row = cursor.next()
    except StopIteration:
        if nullok:
            return None
        raise ValueError('Empty cursor')
    else:
        try:
            cursor.next()
        except StopIteration:
            pass
        else:
            raise ValueError('Multiple-result cursor')
        return row

def cursor_value(cursor, nullok=None):
    row = cursor_row(cursor, nullok)
    if row is None:
        assert nullok
        return None
    if len(row) != 1:
        raise ValueError('Non-unit cursor')
    return row[0]

def json_dumps(obj):
    """Return a JSON string of obj, compactly and deterministically."""
    return json.dumps(obj, sort_keys=True)
