# stdlib
from unittest import TestCase

# project
from tasks.client.filtering import parse_filtering_rule


class TestFiltering(TestCase):
    def test_empty_tags_and_filters(self):
        tag_filters = []
        resource_tags = []
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_empty_filters(self):
        tag_filters = []
        resource_tags = ["datadog:true"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_inclusive_match(self):
        tag_filters = ["datadog:true"]
        resource_tags = ["datadog:true"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_inclusive_nonmatch(self):
        tag_filters = ["datadog:true"]
        resource_tags = []
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_excluding_match(self):
        tag_filters = ["!major:fomo"]
        resource_tags = ["major:fomo"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_excluding_nonmatch(self):
        tag_filters = ["!major:fomo"]
        resource_tags = ["minor:polo"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_both_given(self):
        tag_filters = ["datadog:true", "!major:fomo"]
        resource_tags = ["datadog:true", "major:fomo"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_same_value_given(self):
        tag_filters = ["hi:there", "!hi:there"]
        resource_tags = ["hi:there"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_both_multiples_given_match_include(self):
        tag_filters = ["datadog:true", "env:test", "happy:days", "!major:fomo", "!good:bye", "!bad:times"]
        resource_tags = ["datadog:true"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_both_multiples_given_match_exclude(self):
        tag_filters = ["datadog:true", "env:test", "happy:days", "!major:fomo", "!good:bye", "!bad:times"]
        resource_tags = ["major:fomo"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_both_multiples_given_match_all(self):
        tag_filters = ["datadog:true", "env:test", "happy:days", "!major:fomo", "!good:bye"]
        resource_tags = ["datadog:true", "env:test", "happy:days", "major:fomo", "good:bye"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))
