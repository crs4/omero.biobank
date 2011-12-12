PYTHON = python


.PHONY: all build install install_user clean distclean

all: build

build:
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install --skip-build

install_user: build
	$(PYTHON) setup.py install --skip-build --user

docs: install_user
	make -C docs html

clean:
	rm -rf build
	make -C docs clean
	cd tests/tools/snp_reannotator && bash test_snp_reannotator.sh --clean
	rm -fv $(svn status tests/tools/importer | grep '^?' | awk '{print $2}')
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

distclean: clean
