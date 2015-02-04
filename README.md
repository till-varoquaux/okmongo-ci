[![Build Status](https://travis-ci.org/till-varoquaux/okmongo-ci.png?branch=master)](https://travis-ci.org/till-varoquaux/okmongo-ci)
<!--[![Coverage Status](https://coveralls.io/repos/till-varoquaux/okmongo-ci/badge.svg)](https://coveralls.io/r/till-varoquaux/okmongo-ci)-->

Okmongo
-------

The low-level bson/mongo protocol implementation for developers who never want
to write a mongo driver from scratch again.


Okmongo is a low-level c++ interface to the [mongodb](http://mongodb.org)
protocol. The IO layer and most of the memory management is left up to the user
of the library. Our only aim is to implement the lowest common denominator and
do that right. The rest is up to you...

While this is a less turnkey solution it integrates better in
existing projects that might have their conventions in terms of datastructure
and IO. It is also very low overhead and has just about no dependencies.


Purpose
--------

This library was originally created to be used internally at okcupid in an
asynchronous framework. This library tries really hard to not make you pay for features you do not use and to make no assumptions about the codebase it'll be in.

Unlike the [official driver](https://github.com/mongodb/mongo-cxx-driver) it doesn't strive to be complete and is neither well supported nor well documented.


LICENSE
------
<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">okmongo</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.
