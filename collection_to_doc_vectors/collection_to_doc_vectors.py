import sys
sys.path += ['thirdparty/anserini/src/main/python']
import spacy
from pyserini.collection import pycollection
from pyserini.pyclass import JCollections
from pyserini.index import pygenerator
import subprocess
import json
from tqdm import tqdm


def documents_in_trec_jsoup_collection(directory):
    collection = pycollection.Collection('TrecCollection', directory)
    generator = pygenerator.Generator('JsoupGenerator')

    for (_, fs) in enumerate(collection):
        for (_, document) in enumerate(fs):
            ret = generator.create_document(document)
            if ret is not None:
                yield ret


def transform_documents(config):
    document_transformer = __map_parsed_document(config)
    documents = [i for i in documents_in_trec_jsoup_collection(config['collection_directory'])]
    ret = [document_transformer(i) for i in documents]
    __write_to_file(
        target_file=config['output_file'],
        transformed_documents=(document_transformer(i) for i in documents)
    )

    return ret


def __map_parsed_document(config):
    content_extractor = __main_content_extractor(config)
    content_extractor = __to_word_vectors(config, content_extractor)

    return lambda document: {
        'docid': document.get('id'),
        'content': content_extractor(document)
    }


def __main_content_extractor(config):
    if 'extract_main_content' in config and config['extract_main_content']:
        return lambda document: subprocess.check_output([
            '.venv/bin/python3', 'collection_to_doc_vectors/main_content_extraction.py'
        ], input=document.get('raw').encode('utf-8')).decode("utf-8")

    return lambda document: document.get('raw')


def __to_word_vectors(config, function):
    if 'transform_to_word_vectors' in config and config['transform_to_word_vectors']:
        nlp = spacy.load('en_core_web_lg')
        return lambda document: nlp(function(document)).vector.tolist()

    return lambda document: function(document)


def __write_to_file(target_file, transformed_documents):
    with open(target_file, 'w+') as target:
        for transformed_document in transformed_documents:
            target.write(json.dumps(transformed_document) + '\n')


if __name__ == '__main__':
    for doc in tqdm(documents_in_trec_jsoup_collection('data/robust')):
        docid = doc.get('id')
        raw = doc.get('raw')
        contents = doc.get('contents')
