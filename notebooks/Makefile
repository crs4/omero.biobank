# gnu makes  and bash rule!
#----------------------------------------------------------------

IPYNBFILES := $(wildcard *.ipynb)
RSTFILES := $(patsubst %.ipynb, %.rst, ${IPYNBFILES})
RSTDIRS := $(patsubst %.ipynb, %_files, ${IPYNBFILES})

all: rst

rst: ${RSTFILES}

clean:
	-rm -rf ${RSTFILES} ${RSTDIRS}

startnb:
	ipython notebook --pylab

realclean: clean

.SUFFIXES:
.SUFFIXES: .ipynb .rst

.ipynb.rst:
	ipython nbconvert --to=rst $<

