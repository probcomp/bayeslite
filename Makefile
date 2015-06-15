.PHONY: default-target
default-target: build

###############################################################################
### User-settable variables

# List of documentation formats to generate.
DOCS = \
	$(SPHINX_DOCS) \
	pdf \
	# end of DOCS

# Commands to run in the build process.
PDFLATEX = pdflatex
PYTHON = python
SPHINX_BUILD = sphinx-build
SPHINX_FLAGS =

# Options for above commands.
PDFLATEXOPTS =
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
	latex \
	# end of SPHINX_DOCS

# doc: Build the bayeslite documentation.
.PHONY: doc
doc: $(DOCS)

.PHONY: $(SPHINX_DOCS)
$(SPHINX_DOCS): pythenv.sh build
	rm -rf build/doc/$@ && \
	rm -rf build/doc/$@.tmp && \
	./pythenv.sh $(SPHINX_BUILD) $(SPHINX_FLAGS) -b $@ doc \
	  build/doc/$@.tmp && \
	mv -f build/doc/$@.tmp build/doc/$@

.PHONY: pdf
pdf: latex
	rm -rf build/doc/$@ && \
	rm -rf build/doc/$@.tmp && \
	mkdir build/doc/$@.tmp && \
	{ tar -C build/doc/latex -c -f - . \
	  | tar -C build/doc/$@.tmp -x -f -; } && \
	(cd build/doc/$@.tmp && \
	  $(PDFLATEX) $(PDFLATEXOPTS) \\nonstopmode\\input bayeslite && \
	  $(PDFLATEX) $(PDFLATEXOPTS) \\nonstopmode\\input bayeslite && \
	  $(PDFLATEX) $(PDFLATEXOPTS) \\nonstopmode\\input bayeslite && \
	  $(MAKEINDEX) -s python.ist bayeslite.idx; \
	  $(PDFLATEX) $(PDFLATEXOPTS) \\nonstopmode\\input bayeslite && \
	  $(PDFLATEX) $(PDFLATEXOPTS) \\nonstopmode\\input bayeslite && \
	  :) && \
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
