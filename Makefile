PIP = .venv/bin/pip2
PYTHON = .venv/bin/python


pip-install:
	@rm -Rf .venv &&\
	pip2 install --user virtualenv &&\
	virtualenv -p python2 .venv &&\
	$(PIP) install --upgrade pip &&\
	$(PIP) install Cython &&\
	$(PIP) install spacy pyjnius &&\
	$(PYTHON) -m spacy download en_core_web_lg

