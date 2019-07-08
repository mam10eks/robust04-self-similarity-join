import sys
import os
sys.path += ['../thirdparty/anserini/src/main/python']
from collection_to_doc_vectors.collection_to_doc_vectors import *

OUTPUT_FILE = 'test-output-file'


def delete_output_file():
    delete_file_if_exists(OUTPUT_FILE)


def delete_file_if_exists(file_path):
    try:
        os.remove(file_path)
    except:
        pass


def transform_documents_and_select_entries_by_id(conf):
    conf['output_file'] = OUTPUT_FILE
    transform_documents(conf)
    ret = []

    with open(OUTPUT_FILE, 'r') as file:
        for line in file:
            ret += [json.loads(line)]

    return [i for i in ret if 'ids' not in conf or i['docid'] in conf['ids']]
