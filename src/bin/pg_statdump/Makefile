#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_statdump
#
# Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_statdump/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_statdump - dumps access statistics in PostgreSQL databases"
PGAPPICON=win32

subdir = src/bin/pg_statdump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS=	pg_statdump.o $(WIN32RES)

all: pg_statdump

pg_statdump: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_statdump$(X) '$(DESTDIR)$(bindir)/pg_statdump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_statdump$(X)'

clean distclean maintainer-clean:
	rm -f pg_statdump$(X) $(OBJS)
