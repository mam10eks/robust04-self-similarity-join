import sys
sys.path += ['thirdparty/anserini/src/main/python']
from pyserini.collection import pycollection
from pyserini.pyclass import JCollections
from pyserini.index import pygenerator
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


def transform_documents(conf):
    document_transformer = __map_parsed_document
    documents = [i for i in documents_in_trec_jsoup_collection(conf['collection_directory'])]
    ret = [document_transformer(i) for i in documents]
    __write_to_file(
        target_file=conf['output_file'],
        transformed_documents=(document_transformer(i) for i in documents)
    )

    return ret


def __map_parsed_document(document):
    return {
        'docid': document.get('id'),
        'raw': document.get('raw'),
        'contents': document.get('contents')
    }


def __write_to_file(target_file, transformed_documents):
    with open(target_file, 'w+') as target:
        for transformed_document in transformed_documents:
            target.write(json.dumps(transformed_document) + '\n')


if __name__ == '__main__':
    for doc in tqdm(documents_in_trec_jsoup_collection('data/robust')):
        docid = doc.get('id')
        raw = doc.get('raw')
        contents = doc.get('contents')

