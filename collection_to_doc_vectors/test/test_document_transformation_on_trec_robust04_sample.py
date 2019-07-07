import unittest
import sys
import os
from unittest.mock import patch
sys.path += ['../../thirdparty/anserini/src/main/python']
from collection_to_doc_vectors.collection_to_doc_vectors import *
from approvaltests import verify_as_json

LA_TIMES_SAMPLE = ['LA010189-0001', 'LA010189-0043', 'LA010189-0097', 'LA010189-0152', 'LA010189-0192']


class TestDocumentTransformationOnTrecRobust04Sample(unittest.TestCase):

    def test_no_transformation(self):
        transformed_document_sample = self.transform_documents_and_select_entries_by_id({
            'test_directory': '../test/data/robust',
            'ids': LA_TIMES_SAMPLE
        })

        verify_as_json(transformed_document_sample)

    @staticmethod
    def transform_documents_and_select_entries_by_id(conf):
        ret = transform_documents(conf['test_directory'])

        return [i for i in ret if i['docid'] in conf['ids']]
