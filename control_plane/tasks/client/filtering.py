# stdlib
from collections.abc import Iterable
from fnmatch import translate
from functools import reduce
from re import Match, Pattern, compile

# https://github.com/DataDog/dogweb/blob/prod/integration/common/filtering.py
# This file is taken from dogweb to ensure consistency in filtering logic


def parse_filtering_rule(filter_strs, all_filters=False):
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


def accept(_):
    """A predicate that is always satisfied"""
    return True


def reject(_):
    """A predicate that is never satisfied"""
    return False


def p_or(pred0, pred1):
    """Create a predicate that is satisfied when either given predicate is satisfied"""
    return lambda v: pred0(v) or pred1(v)


def p_and(preds):
    """Create a predicate that is satisfied when all given predicates are satisfied"""
    return lambda v: all(pred(v) for pred in preds)


def p_not(pred):
    """Create a predicate that is satisfied when the given predicate is not satisfied"""
    return lambda v: not pred(v)


def p_any(preds):
    """Create a predicate that is satisfied when any of the given predicates are satisfied"""
    return reduce(lambda acc, p: p_or(p, acc), preds, reject)


def p_none(preds):
    """Create a predicate that is satisfied when none of the given predicates are satisfied"""
    return p_not(p_any(preds))


def p_all(preds):
    """Create a predicate that is satisfied when all of the given predicates are satisfied"""
    return p_none(list(map(p_not, preds)))


class FilteringRule:
    """A predicate that is satisfied when at least one acceptance predicate is satisfied
    and no rejection predicates are satisfied.  If no acceptance predicates are specified and no
    rejection predicates are satisfied, this predicate accepts.

    :param accept_preds: [[Tag] -> bool] - list of predicates that define acceptance criteria
    :param reject_preds: [[Tag] -> bool] - list of predicates that define rejection criteria
    :param all_filters: [bool] - if true, all accept_preds must be fulfilled, not just any
    """

    def __init__(self, accept_preds, reject_preds, all_filters=False):
        self.accept_preds = accept_preds
        self.reject_preds = reject_preds
        self.all_filters = all_filters

    @property
    def pred(self):
        accept_cond = p_and if self.all_filters else p_any
        return p_and((accept_cond(self.accept_preds or [accept]), p_none(self.reject_preds)))

    def __call__(self, tags):
        return self.pred(tags)


def create_filtering_rule(accept_preds=None, reject_preds=None, all_filters=False):
    """Create a predicate that is satisfied when at least one acceptance predicate is satisfied
    and no rejection predicates are satisfied.  If no acceptance predicates are specified and no
    rejection predicates are satisfied, this predicate accepts.

    :param accept_preds: [[Tag] -> bool]? - list of predicates that define acceptance criteria
    :param reject_preds: [[Tag] -> bool]? - list of predicates that define rejection criteria
    :param all_filters: [bool] - if true, rule will require all rules to match, not any
    """
    if accept_preds is None:
        accept_preds = []
    if reject_preds is None:
        reject_preds = []
    return FilteringRule(accept_preds, reject_preds, all_filters)


def _sanitize(string):
    return string.lower().strip()


def _tags_match_filter(filter_str, tags):
    return len(filter_to_wildcard_match(map(_sanitize, tags), _sanitize(filter_str))) > 0


def _is_accept_pred(filter_str):
    return not filter_str.startswith("!")


def _is_reject_pred(filter_str):
    return filter_str.startswith("!")


def _parse_accept_pred(filter_str):
    return lambda tags: _tags_match_filter(filter_str, tags)


def _parse_reject_pred(filter_str):
    return lambda tags: _tags_match_filter(filter_str[1:], tags)


def _parse_accept_preds(filter_strs):
    return [_parse_accept_pred(pred) for pred in filter_strs if _is_accept_pred(pred)]


def _parse_reject_preds(filter_strs):
    return [_parse_reject_pred(pred) for pred in filter_strs if _is_reject_pred(pred)]
