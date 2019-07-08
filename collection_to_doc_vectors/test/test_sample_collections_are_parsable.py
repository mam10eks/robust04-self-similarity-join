from .test_util import *
import unittest
from approvaltests import verify_as_json

OUTPUT_FILE = 'test-output-file'
LA_TIMES_SAMPLE = ['LA010189-0001', 'LA010189-0043', 'LA010189-0097', 'LA010189-0152', 'LA010189-0192']
CLUEWEB_SAMPLE = ['clueweb09-en0000-00-00000', 'clueweb09-en0000-00-00010', 'clueweb09-en0000-00-00021']


class TestSampleCollectionsAreParsable(unittest.TestCase):

    def setUp(self):
        delete_output_file()

    def tearDown(self):
        self.setUp()

    def test_parsing_of_clueweb09_without_transformation(self):
        transformed_document_sample = transform_documents_and_select_entries_by_id(
            conf=self.parameters(
                collection_directory='collection_to_doc_vectors/test/data/clueweb09-sample',
                pyserini_collection='ClueWeb09Collection',
                pyserini_generator='JsoupGenerator',
                sample=CLUEWEB_SAMPLE
            )
        )

        verify_as_json(transformed_document_sample)

    def test_parsing_of_robust04_without_transformation(self):
        transformed_document_sample = transform_documents_and_select_entries_by_id(
            conf=self.parameters(
                collection_directory='collection_to_doc_vectors/test/data/robust',
                pyserini_collection='TrecCollection',
                pyserini_generator='JsoupGenerator',
                sample=LA_TIMES_SAMPLE
            )
        )
        
        verify_as_json(transformed_document_sample)

    @staticmethod
    def parameters(collection_directory, pyserini_collection, pyserini_generator, sample):
        return {
            'collection_directory': collection_directory,
            'ids': sample,
            'pyserini_collection': pyserini_collection,
            'pyserini_generator': pyserini_generator,
            'extract_main_content': False,
            'transform_to_word_vectors': False,
        }
