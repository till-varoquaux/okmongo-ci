#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

dnl This file works with version of autoconf and automake as old as 2.67 and
dnl 1.11 to maintain compatibility with debian squeeze but 2.69 and 1.13 are
dnl preferred
AC_PREREQ([2.67])

AC_INIT(okmongo, 0.1, till@okcupid.com)
AC_SUBST(LIBVERSION, [0:1:0])
AC_CONFIG_AUX_DIR([config])
AM_INIT_AUTOMAKE([1.11])

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_SRCDIR([src/string_matcher.h])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
LT_INIT
AC_SUBST([LIBTOOL_DEPS])

AC_PROG_CXX([clang++ g++])
AC_LANG([C++])

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h netinet/in.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
dnl this macro is not defined in old autoconfs...
ifdef(AC_CHECK_HEADER_STDBOOL, [AC_CHECK_HEADER_STDBOOL], [])
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
dnl AC_TYPE_SIZE_T
dnl AC_TYPE_SSIZE_T
dnl AC_TYPE_UINT16_T
dnl AC_TYPE_UINT32_T
dnl AC_TYPE_UINT64_T
dnl AC_TYPE_UINT8_T

# Checks for library functions.
AC_CHECK_FUNCS([bzero socket])

dnl c++11
dnl there might be a WError or some other flag in there that would cause this
dnl macro to fail.
m4_include([m4/ax_cxx_compile_stdcxx_11.m4])
ok_save_CXXFLAGS="${CXXFLAGS}"
CXXFLAGS=""
AX_CXX_COMPILE_STDCXX_11(noext)
CXXFLAGS="${ok_save_CXXFLAGS} ${CXXFLAGS}"

dnl check whether we are using clang...
AC_MSG_CHECKING([if compiling with clang])

AC_COMPILE_IFELSE(
[AC_LANG_PROGRAM([], [[
#ifndef __clang__
       not clang
#endif
]])],
[CLANG=yes], [CLANG=no])

AC_MSG_RESULT([$CLANG])
dnl

dnl---------- dev tools ----------------------
dnl doxygen support
m4_include([m4/ax_prog_doxygen.m4])
DX_HTML_FEATURE(ON)
DX_CHM_FEATURE(OFF)
DX_CHI_FEATURE(OFF)
DX_MAN_FEATURE(OFF)
DX_RTF_FEATURE(OFF)
DX_XML_FEATURE(OFF)
DX_PDF_FEATURE(OFF)
DX_PS_FEATURE(OFF)
DX_INIT_DOXYGEN($PACKAGE_NAME, doxyconfig)
dnl

dnl clang format
dnl needs automake > =1.13
ifdef([AM_EXTRA_RECURSIVE_TARGETS],
    [
        AC_ARG_VAR([CLANG_FORMAT], [clang-format command])
        AC_CHECK_PROGS(CLANG_FORMAT, clang-format clang-format-3.7 clang-format-3.6 clang-format-3.5 clang-format-3.4, )
        AS_IF(
            [test "x${CLANG_FORMAT}" != "x"],
            [AM_EXTRA_RECURSIVE_TARGETS([format-src])]
        )
    ],
    [
        CLANG_FORMAT=
        AC_SUBST(CLANG_FORMAT)
    ]
)

AM_CONDITIONAL([HAS_CLANG_FORMAT], [test "x${CLANG_FORMAT}" != x""])

dnl dev_mode turns on a lot of debug flags and the clang sanitizers...
AC_MSG_CHECKING(if dev-mode is enabled)

ANALYZE=no

AC_ARG_ENABLE([coverage],
[AS_HELP_STRING([--enable-coverage],
[enable coverage report for test suite])],
[COVERAGE=$enableval],
[COVERAGE=no])

AC_ARG_ENABLE(
    dev-mode,
    AS_HELP_STRING(
        [--enable-dev-mode],
        [Useful only for developers of the library]
    ),
    [
        AC_MSG_RESULT(yes)
        AS_IF(
            [test "x${CLANG}" = "xno"],
            [AC_MSG_ERROR([dev-mode requires you to use clang])]
        )

        dnl this is hackish but the doxygen m4 macros don't offer any good way
        dnl to check for this in autoconf
        AS_IF(
            [test "x${DX_COND_doc_TRUE}" != "x"],
            [AC_MSG_ERROR([dev-mode requires doxygen])]
        )
        AS_IF(
            [test "x${CLANG_FORMAT}" = "x"],
            [AC_MSG_ERROR([dev-mode requires clang-format])]
        )

        CXXFLAGS+=" -Weverything -Wno-c++98-compat -Wno-padded -Wno-switch-enum"
        CXXFLAGS+=" -Werror -fsanitize=address,undefined,integer"
        CXXFLAGS+=" -fno-omit-frame-pointer"
        LDFLAGS+=" -fsanitize=address,undefined,integer"
        ANALYZE=yes
        DEBUG=yes
    ],
    AC_MSG_RESULT(no)
)

dnl clang analyzer
AM_CONDITIONAL([RUN_CLANG_ANALYZE], [test "x${ANALYZE}" = x"yes"])
AM_CONDITIONAL(ENABLE_COVERAGE, test "x$COVERAGE" = "xyes")

if test "x$COVERAGE" = "xyes"; then
    DEBUG=yes
fi

if test "x$DEBUG" = "xyes"; then
    # Remove all optimization flags from CFLAGS taken from libsass's
    # configure.ac
    changequote({,})
    CXXFLAGS="$(echo "$CXXFLAGS" | $SED -e 's/-O[0-9]* *//g')"
    changequote([,])
    CXXFLAGS+=" -O0 -g"
fi

dnl---------------------------------------

AC_SUBST(LDFLAGS)
AC_SUBST(CXXFLAGS)

AC_OUTPUT([Makefile src/Makefile examples/Makefile])
