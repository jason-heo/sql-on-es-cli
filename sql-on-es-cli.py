#!/usr/bin/env python
# -*- coding: utf8 -*-

import readline
import os

hist_file = os.path.join(os.path.expanduser("~"), ".sql-on-es.hist")

try:
    readline.read_history_file(hist_file)
except (IOError):
    pass

import atexit
atexit.register(readline.write_history_file, hist_file)

# readline.parse_and_bind('tab: complete')
# readline.parse_and_bind('set editing-mode emacs')

while True:
    try:
        line = raw_input('SQL> ')
        print 'Entered: "%s"' % line
    except (EOFError):
        break

