# gnu makes  and bash rules!
#----------------------------------------------------------------

IPYNBFILES := $(wildcard *.ipynb)
RSTFILES := $(patsubst %.ipynb, %.rst, ${IPYNBFILES})
RSTDIRS := $(patsubst %.ipynb, %_files, ${IPYNBFILES})

BIOBANK_CONFIG=./.biobank

all: rst

rst: ${RSTFILES}

clean:
	-rm -rf ${RSTFILES} ${RSTDIRS}

startnb:
	if test -e ${BIOBANK_CONFIG}; then source ${BIOBANK_CONFIG}; fi; \
        ipython notebook --pylab

realclean: clean

.SUFFIXES:
.SUFFIXES: .ipynb .rst

.ipynb.rst:
	ipython nbconvert --to=rst $<

