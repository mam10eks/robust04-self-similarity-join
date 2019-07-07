import os, sys
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


if __name__ == '__main__':
    for doc in tqdm(documents_in_trec_jsoup_collection('data/robust')):
        docid = doc.get('id')
        raw = doc.get('raw')
        contents = doc.get('contents')

