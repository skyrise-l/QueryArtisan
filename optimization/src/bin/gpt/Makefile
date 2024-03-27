# src/bin/gpt/Makefile

PGFILEDESC = "gpt - my gpt project"
PGAPPICON = win32

subdir = src/bin/gpt
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	gpt.o

all: gpt

gpt: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

gpt.o: gpt.c	
	$(CC) $(CFLAGS) -c gpt.c -o gpt.o

install: all installdirs
	$(INSTALL_PROGRAM) gpt$(X) '$(DESTDIR)$(bindir)/gpt$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gpt$(X)'

clean distclean maintainer-clean:
	rm -f gpt$(X) $(OBJS)


