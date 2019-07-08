PIP = .venv/bin/pip3
PYTHON = .venv/bin/python3


create-robust-document-vectors:
	$(PYTHON) collection_to_doc_vectors/collection_to_doc_vectors.py \
		--transform_to_word_vectors True\
		--extract_main_content True\
		--pyserini_collection TrecCollection\
		--pyserini_generator JsoupGenerator\
		--collection_directory ../ltr-simulation/data/robust/\
		--output_file robust-main-content-word-vectors.jsonl


create-clueweb-document-vectors:
	$(PYTHON) collection_to_doc_vectors/collection_to_doc_vectors.py \
		--transform_to_word_vectors True\
		--extract_main_content True\
		--pyserini_collection ClueWeb09Collection\
		--pyserini_generator JsoupGenerator\
		--collection_directory ../ltr-simulation/data/ClueWeb09_English_1/\
		--output_file clueweb09-main-content-word-vectors.jsonl


test:
	.venv/bin/nosetests

install: checkout-submodules
	@rm -Rf .venv &&\
	python3 -m venv .venv &&\
	$(PIP) install --upgrade pip &&\
	$(PIP) install Cython &&\
	$(PIP) install spacy pyjnius approvaltests nose &&\
	$(PIP) install thirdparty/python-poilerpipe/ &&\
	$(PYTHON) -m spacy download en_core_web_lg &&\
	mvn -f thirdparty/anserini clean package appassembler:assemble -DskipTests &&\
	mvn -f trec-ndd install

checkout-submodules:
	@git submodule update --init --recursive

