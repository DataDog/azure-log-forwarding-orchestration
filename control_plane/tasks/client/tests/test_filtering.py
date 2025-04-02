# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from unittest import TestCase

# project
from tasks.client.filtering import p_all, parse_filtering_rule


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

    def test_multiple_inclusive_match_one(self):
        tag_filters = ["datadog:true", "env:test"]
        resource_tags = ["datadog:true"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_inclusive_nonmatch(self):
        tag_filters = ["datadog:true"]
        resource_tags = []
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_excluding_match(self):
        tag_filters = ["!major:fomo"]
        resource_tags = ["major:fomo"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_excluding_nonmatch(self):
        tag_filters = ["!major:fomo"]
        resource_tags = []
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_single_both_given(self):
        tag_filters = ["datadog:true", "!major:fomo"]
        resource_tags = ["datadog:true", "major:fomo"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_multiple_excluding_match_one(self):
        tag_filters = ["!major:fomo", "!no:sir"]
        resource_tags = ["major:fomo"]
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

    def test_star_filter(self):
        tag_filters = ["datadog:*"]
        resource_tags = ["datadog:true"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

    def test_question_mark_filter(self):
        tag_filters = ["datadog:?"]
        resource_tags = ["datadog:t"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

        resource_tags = ["datadog:trex"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_key_only_filter(self):
        tag_filters = ["datadog"]
        resource_tags = ["datadog"]
        self.assertTrue(parse_filtering_rule(tag_filters)(resource_tags))

        resource_tags = ["datadog:true"]
        self.assertFalse(parse_filtering_rule(tag_filters)(resource_tags))

    def test_p_all(self):
        def has_datadog(tags):
            return "datadog:true" in tags

        def has_env(tags):
            return "env:test" in tags

        def has_happy_days(tags):
            return "happy:days" in tags

        all_predicate = p_all([has_datadog, has_env, has_happy_days])
        self.assertTrue(all_predicate(["datadog:true", "env:test", "happy:days"]))
        self.assertFalse(all_predicate(["datadog:true", "env:test"]))
