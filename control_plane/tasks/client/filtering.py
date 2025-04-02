# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import Callable, Iterable
from fnmatch import translate
from functools import reduce
from re import Match, Pattern, compile
from typing import Any, TypeAlias

# Refer to details: https://datadoghq.atlassian.net/wiki/x/JQwJIwE

Predicate: TypeAlias = Callable[[list[str]], bool]


class FilteringRule:
    """A predicate that is satisfied when at least one acceptance predicate is satisfied
    and no rejection predicates are satisfied.  If no acceptance predicates are specified and no
    rejection predicates are satisfied, this predicate accepts.
    :param accept_preds: list[Predicate] | None - list of predicates that define acceptance criteria
    :param reject_preds: list[Predicate] | None - list of predicates that define rejection criteria
    :param all_filters: bool - if true, all accept_preds must be fulfilled, not just any
    """

    def __init__(
        self,
        accept_preds: list[Predicate] | None = None,
        reject_preds: list[Predicate] | None = None,
        all_filters: bool = False,
    ):
        self.accept_preds = accept_preds or []
        self.reject_preds = reject_preds or []
        self.all_filters = all_filters

    @property
    def pred(self) -> Predicate:
        accept_cond = p_and if self.all_filters else p_any
        return p_and([accept_cond(self.accept_preds or [accept]), p_none(self.reject_preds)])

    def __call__(self, tags: list[str]) -> bool:
        return self.pred(tags)


def create_filtering_rule(
    accept_preds: list[Predicate] | None = None,
    reject_preds: list[Predicate] | None = None,
    all_filters: bool = False,
) -> FilteringRule:
    """Create a predicate that is satisfied when at least one acceptance predicate is satisfied
    and no rejection predicates are satisfied.  If no acceptance predicates are specified and no
    rejection predicates are satisfied, this predicate accepts.
    :param accept_preds: list[Predicate] | None - list of predicates that define acceptance criteria
    :param reject_preds: list[Predicate] | None - list of predicates that define rejection criteria
    :param all_filters: bool - if true, rule will require all rules to match, not any
    """
    return FilteringRule(accept_preds, reject_preds, all_filters)


def parse_filtering_rule(filter_strs: list[str], all_filters: bool = False) -> FilteringRule:
    """Parses tag filter string and returns a filtering rule that can be invoked with a set of resource tags.
    Filtering rule call returns true if the resource should be filtered in, false if it should be filtered out.
    """
    return create_filtering_rule(
        accept_preds=_parse_accept_preds(filter_strs),
        reject_preds=_parse_reject_preds(filter_strs),
        all_filters=all_filters,
    )


def _compile_wildcard(pattern: str) -> Pattern:
    return compile(translate(pattern))


def get_wildcard_match(name: str, pattern: str, ignore_case: bool = True) -> Match | None:
    return _compile_wildcard(pattern.lower() if ignore_case else pattern).match(name.lower() if ignore_case else name)


def filter_to_wildcard_match(names: Iterable[str], pattern: str, ignore_case: bool = True) -> list[str]:
    return [name for name in names if get_wildcard_match(name, pattern, ignore_case=ignore_case)]


def accept(_: Any) -> bool:
    """A predicate that is always satisfied"""
    return True


def reject(v: Any) -> bool:
    """A predicate that is never satisfied"""
    return False


def p_or(pred0: Predicate, pred1: Predicate) -> Predicate:
    """Create a predicate that is satisfied when either given predicate is satisfied"""
    return lambda v: pred0(v) or pred1(v)


def p_and(preds: list[Predicate]) -> Predicate:
    """Create a predicate that is satisfied when all given predicates are satisfied"""
    return lambda v: all(pred(v) for pred in preds)


def p_not(pred: Predicate) -> Predicate:
    """Create a predicate that is satisfied when the given predicate is not satisfied"""
    return lambda v: not pred(v)


def p_any(preds: list[Predicate]) -> Predicate:
    """Create a predicate that is satisfied when any of the given predicates are satisfied"""
    return reduce(lambda acc, p: p_or(p, acc), preds, reject)


def p_none(preds: list[Predicate]) -> Predicate:
    """Create a predicate that is satisfied when none of the given predicates are satisfied"""
    return p_not(p_any(preds))


def p_all(preds: list[Predicate]) -> Predicate:
    """Create a predicate that is satisfied when all of the given predicates are satisfied"""
    return p_none(list(map(p_not, preds)))


def _sanitize(string: str) -> str:
    return string.lower().strip()


def _tags_match_filter(filter_str: str, tags: list[str]) -> bool:
    return len(filter_to_wildcard_match(map(_sanitize, tags), _sanitize(filter_str))) > 0


def _is_accept_pred(filter_str: str) -> bool:
    return not filter_str.startswith("!")


def _is_reject_pred(filter_str: str) -> bool:
    return filter_str.startswith("!")


def _parse_accept_pred(filter_str: str) -> Predicate:
    return lambda tags: _tags_match_filter(filter_str, tags)


def _parse_reject_pred(filter_str: str) -> Predicate:
    return lambda tags: _tags_match_filter(filter_str[1:], tags)


def _parse_accept_preds(filter_strs: list[str]) -> list[Predicate]:
    return [_parse_accept_pred(pred) for pred in filter_strs if _is_accept_pred(pred)]


def _parse_reject_preds(filter_strs: list[str]) -> list[Predicate]:
    return [_parse_reject_pred(pred) for pred in filter_strs if _is_reject_pred(pred)]
