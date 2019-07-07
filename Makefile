PIP = .venv/bin/pip3
PYTHON = .venv/bin/python3


create-document-vectors:
	$(PYTHON) collection_to_doc_vectors/collection_to_doc_vectors.py

install: checkout-submodules
	@rm -Rf .venv &&\
	python3 -m venv .venv &&\
	$(PIP) install --upgrade pip &&\
	$(PIP) install Cython &&\
	$(PIP) install spacy pyjnius approvaltests &&\
	$(PYTHON) -m spacy download en_core_web_lg &&\
	mvn -f thirdparty/anserini clean package appassembler:assemble -DskipTests

checkout-submodules:
	@git submodule update --init --recursive

