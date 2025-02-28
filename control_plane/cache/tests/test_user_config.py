# stdlib
from logging import Logger
from unittest import TestCase

from cache.user_config import convert_pii_rules_to_json


class TestUserConfig(TestCase):
    def test_empty_pii_rules(self):
        rules = ""

        actual_json = convert_pii_rules_to_json(rules, Logger("test"))
        expected_json = "{}"

        self.assertEqual(actual_json, expected_json)

    def test_valid_pii_rules(self):
        rules = r"""
        rule1:
            pattern: 'hi'
            replacement: 'bye'
        rule_2:
          pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'  
          replacement: 'redactedemail@redacted.com'
        rule-3:
            pattern: '^\+?[1-9]\d{0,2}[-.\s]?(\(?\d{1,4}\)?)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}$'
            replacement: 'redactedphone'
        """

        actual_json = convert_pii_rules_to_json(rules, Logger("test"))
        expected_json = r'{"rule1": {"pattern": "hi", "replacement": "bye"}, "rule_2": {"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", "replacement": "redactedemail@redacted.com"}, "rule-3": {"pattern": "^\\+?[1-9]\\d{0,2}[-.\\s]?(\\(?\\d{1,4}\\)?)?[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,9}$", "replacement": "redactedphone"}}'

        self.assertEqual(actual_json, expected_json)

    def test_invalid_pii_rules(self):
        rules = r"""
        rule1:
          pattern: 'hi'
            replacement: 'bye'
        """

        actual_json = convert_pii_rules_to_json(rules, Logger("test"))
        expected_json = "{}"

        self.assertEqual(actual_json, expected_json)
