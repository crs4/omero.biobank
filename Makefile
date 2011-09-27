

.PHONY: all build install install_user clean distclean

all: build

build:
	python setup.py build

install: build
	python setup.py install --skip-build

install_user: build
	python setup.py install --skip-build --user

clean:
	rm -rf build
	make -C docs clean
	cd tests/tools/snp_reannotator && bash test_snp_reannotator.sh --clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

distclean: clean
