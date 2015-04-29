.PHONY: default-target
default-target: build

###############################################################################
### User-settable variables

# List of documentation formats to generate.
DOCS = \
	$(SPHINX_DOCS) \
	# end of DOCS

# Commands to run in the build process.
PYTHON = python
SPHINX_BUILD = sphinx-build

# Options for above commands.
SPHINXOPTS =
PYTHONOPTS =
SETUPPYOPTS =

###############################################################################
### Targets

# build: Build bayeslite.
.PHONY: build
build: setup.py
	$(PYTHON) $(PYTHONOPTS) setup.py $(SETUPPYOPTS) build

# List of documentation formats we can generate with Sphinx.  These
# should be the formats that have been tested and confirmed to yield
# reasonable output.
SPHINX_DOCS = \
	html \
	# end of SPHINX_DOCS

# doc: Build the bayeslite documentation.
.PHONY: doc
doc: $(DOCS)

.PHONY: $(SPHINX_DOCS)
$(SPHINX_DOCS): pythenv.sh build
	rm -rf build/doc/$@ && \
	rm -rf build/doc/$@.tmp && \
	./pythenv.sh $(SPHINX_BUILD) -b $@ doc build/doc/$@.tmp && \
	mv -f build/doc/$@.tmp build/doc/$@

# check: (Build bayeslite and) run the tests.
.PHONY: check
check: check.sh
	./check.sh

# clean: Remove build products.
.PHONY: clean
clean:
	-rm -rf build
	-rm -rf build/doc/*.tmp
