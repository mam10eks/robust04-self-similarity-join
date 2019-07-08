from .test_util import *
import unittest
from approvaltests import verify_as_json

LA_TIMES_SAMPLE = ['LA010189-0001', 'LA010189-0043', 'LA010189-0097', 'LA010189-0152', 'LA010189-0192']


class TestDocumentTransformationOnTrecRobust04Sample(unittest.TestCase):

    def setUp(self):
        delete_output_file()

    def tearDown(self):
        self.setUp()

    def test_no_transformation(self):
        transformed_document_sample = transform_documents_and_select_entries_by_id(
            conf=self.parameters()
        )

        verify_as_json(transformed_document_sample)

    def test_transformation_to_word_vectors(self):
        transformed_document_sample = transform_documents_and_select_entries_by_id(
            conf=self.parameters(transform_to_word_vectors=True)
        )

        verify_as_json(transformed_document_sample)

    def test_with_main_content_extraction(self):
        transformed_document_sample = transform_documents_and_select_entries_by_id(
            conf=self.parameters(extract_main_content=True)
        )

        verify_as_json(transformed_document_sample)

    def test_with_main_content_extraction_and_word_vectors(self):
        transformed_document_sample = transform_documents_and_select_entries_by_id(
            conf=self.parameters(
                extract_main_content=True,
                transform_to_word_vectors=True
            )
        )

        verify_as_json(transformed_document_sample)

    @staticmethod
    def parameters(extract_main_content=False, transform_to_word_vectors=False):
        return {
            'collection_directory': 'collection_to_doc_vectors/test/data/robust',
            'ids': LA_TIMES_SAMPLE,
            'pyserini_collection': 'TrecCollection',
            'pyserini_generator': 'JsoupGenerator',
            'extract_main_content': extract_main_content,
            'transform_to_word_vectors': transform_to_word_vectors,
        }
