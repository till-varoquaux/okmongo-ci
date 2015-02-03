#! /bin/sh
set -e

libtoolize --force
aclocal
autoheader
automake --force-missing --add-missing
autoconf
#autoreconf --install
