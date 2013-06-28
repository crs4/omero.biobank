PYTHON = python
PY_V := $(shell $(PYTHON) -c 'import sys; print "%d.%d" % sys.version_info[:2]')

EXPORT_DIR = svn_export
APP = $(shell cat NAME)
COPYRIGHT_OWNER = CRS4
NOTICE_TEMPLATE = notice_template.txt
COPYRIGHTER = copyrighter -p $(APP) -n $(NOTICE_TEMPLATE) $(COPYRIGHT_OWNER)
# install copyrighter >=0.4.0 from ac-dc/tools/copyrighter

GENERATED_FILES = AUTHORS MANIFEST README bl/vl/version.py


.PHONY: all build install install_user clean distclean uninstall_user

all: build

build:
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install --skip-build

install_user: build
	$(PYTHON) setup.py install --skip-build --user

docs: install_user
	make -C docs html

dist: docs
	rm -rf $(EXPORT_DIR) && svn export . $(EXPORT_DIR)
	$(COPYRIGHTER) -r $(EXPORT_DIR)
	rm -rf $(EXPORT_DIR)/docs/*
	mv docs/_build/html $(EXPORT_DIR)/docs/
	cd $(EXPORT_DIR) && python setup.py sdist -k

clean:
	rm -rf build
	rm -f $(GENERATED_FILES)
	make -C docs clean
	cd test/tools/snp_manager && bash run_test.sh --clean
	rm -f `svn status test/tools/importer | grep '^?' | awk '{print $2}'`
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

distclean: clean
	rm -rf $(EXPORT_DIR) dist docs/_build

uninstall_user:
	rm -rf ~/.local/lib/python$(PY_V)/site-packages/bl/vl
	rm -rf ~/.local/lib/python$(PY_V)/site-packages/biobank*
