import sys
sys.path += ['thirdparty/anserini/src/main/python']
import spacy
from pyserini.collection import pycollection
from pyserini.pyclass import JCollections
from pyserini.index import pygenerator
import subprocess
import json
import argparse
from tqdm import tqdm


def __documents_in_collection(config):
    collection = pycollection.Collection(config['pyserini_collection'], config['collection_directory'])
    generator = pygenerator.Generator(config['pyserini_generator'])

    for (_, fs) in enumerate(collection):
        for (_, document) in enumerate(fs):
            if not __document_is_parsable(document):
                continue

            ret = generator.create_document(document)
            if ret is not None:
                yield ret


def __document_is_parsable(document):
    return not hasattr(document, 'object') \
           or not hasattr(document.object, 'indexable') \
           or document.object.indexable()


def transform_documents(config):
    document_transformer = __map_parsed_document(config)
    transformed_documents = (document_transformer(i) for i in __documents_in_collection(config))

    __write_to_file(
        target_file=config['output_file'],
        transformed_documents=transformed_documents
    )


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
        for transformed_document in tqdm(transformed_documents):
            target.write(json.dumps(transformed_document) + '\n')


def __argument_parser():
    ret = argparse.ArgumentParser(description='TBD ;)')

    ret.add_argument(
        '--transform_to_word_vectors',
        help='TBD;)',
        required=False,
        type=bool,
        default=False
    )

    ret.add_argument(
        '--extract_main_content',
        help='TBD;)',
        required=False,
        type=bool,
        default=False
    )

    ret.add_argument(
        '--pyserini_collection',
        help='TBD;)',
        required=True,
        type=str,
    )

    ret.add_argument(
        '--pyserini_generator',
        help='TBD;)',
        required=True,
        type=str,
    )

    ret.add_argument(
        '--collection_directory',
        help='TBD;)',
        required=True,
        type=str,
    )

    ret.add_argument(
        '--output_file',
        help='TBD;)',
        required=True,
        type=str,
    )

    return ret


if __name__ == '__main__':
    args = __argument_parser().parse_args()
    transform_documents(vars(args))

