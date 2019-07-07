import sys
sys.path += ['thirdparty/anserini/src/main/python']
from pyserini.collection import pycollection
from pyserini.pyclass import JCollections
from pyserini.index import pygenerator
from tqdm import tqdm


def documents_in_trec_jsoup_collection(directory):
    collection = pycollection.Collection('TrecCollection', directory)
    generator = pygenerator.Generator('JsoupGenerator')

    for (i, fs) in enumerate(collection):
        for (i, doc) in enumerate(fs):
            ret = generator.create_document(doc)
            if ret is not None:
                yield ret


def transform_documents(dir):
    ret = [__map_parsed_document(i) for i in documents_in_trec_jsoup_collection(dir)]

    return ret


def __map_parsed_document(document):
    return {
        'docid': document.get('id'),
        'raw': document.get('raw'),
        'contents': document.get('contents')
    }


if __name__ == '__main__':
    for doc in tqdm(documents_in_trec_jsoup_collection('data/robust')):
        docid = doc.get('id')
        raw = doc.get('raw')
        contents = doc.get('contents')

