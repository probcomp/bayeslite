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

def pp_cursor(out, cursor):
    if not cursor.description:
        return
    labels = [d[0] for d in cursor.description]
    table = cursor.fetchall()
    pp_list(out, table, labels)

def pp_list(out, table, labels):
    assert 0 < len(labels)
    # XXX Consider quotation/escapes.
    colwidths = [len(label) for label in labels]
    for row in table:
        for colno, v in enumerate(row):
            # XXX Consider quotation/escapes.
            # XXX Combining characters?
            colwidths[colno] = max(colwidths[colno], len(unicode(v)))
    first = True
    for colno, label in enumerate(labels):
        if first:
            first = False
        else:
            out.write(' | ')
        # XXX Quote/escape.
        out.write('%*s' % (colwidths[colno], label))
    out.write('\n')
    first = True
    for colno, label in enumerate(labels):
        if first:
            first = False
        else:
            out.write('-+-')
        # XXX Quote/escape.
        out.write('%s' % ('-' * colwidths[colno]))
    out.write('\n')
    for row in table:
        first = True
        for colno, v in enumerate(row):
            if first:
                first = False
            else:
                out.write(' | ')
            # XXX Quote/escape.
            out.write('%*s' % (colwidths[colno], unicode(v)))
        out.write('\n')
