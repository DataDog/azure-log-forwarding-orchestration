# Contributing

## Contributions are welcomed

Pull requests for bug fixes are welcome, but before submitting new features or changes to current
functionality, please [open a support case](https://help.datadoghq.com/hc/en-us/requests/new)
and discuss your ideas or propose the changes you wish to make first to verify the feature is not already in progress. After a confirming Datadog is not already working on it, a [PR can be
submitted](#pull-request-guidelines) for review. We do not accept issues on this repository to ensure all issues are tracked in our internal system.

## Code contributions

### Automatic code formatting

We have automatic code formatting enabled using [pre-commit](https://pre-commit.com/#install) for the python, go and yaml in this repository. Make sure to install it and run `pre-commit install` before committing changes.

## Pull request guidelines

### Draft first

When opening a pull request, please open it as a [draft](https://github.blog/2019-02-14-introducing-draft-pull-requests/) to not auto-assign reviewers before the pull request is in a reviewable state.

### Title format

Pull request titles should briefly describe the proposed changes in a way that makes sense for the users.
They should be a sentence starting with an infinitive verb and avoid using prefixes like `[PROD]` or `PROD - ` in favor of [labels](#labels).

>[!CAUTION]
> Don't title:
> * _Another bug fix_: it doesn't describe the change
> * _Span sampling bug fix_: it doesn't start with an infinite verb
> * _Fix off-by-one error from rule parsing_: it doesn't make sense for the user
> * _[CORE] Fix span sampling rule parsing_: it doesn't use label for component tagging
> * _Fix span sampling rule parsing when using both remote config and property config_: it doesn't fit and will be cut during changelog generation

>[!TIP]
> Do instead: _Fix span sampling rule parsing_

### Labels

GitHub labels applies to pull requests.
They are used to identify the related components using [the `comp: ` category](https://github.com/DataDog/azure-log-forwarding-orchestration/labels?q=comp%3A).

Pull requests should be labelled with at least a component, in addition to the type of changes using [the `type: ` category](https://github.com/DataDog/azure-log-forwarding-orchestration/labels?q=type).

>[!TIP]
> Always add a `comp:` and a `type:` label.

>[!NOTE]
> For reference, the [full list of all labels available](https://github.com/DataDog/dd-trace-java/labels).

## Pull request reviews

### Review expectations

After making your pull request ready for review by converting it from a draft, you can expect to get an initial review comment within two working days and a full review within a week of work.
If you don't receive any update, feel free to send a nice reminder to the assigned reviewers using pull request comments.

### Stale pull requests

A pull request is considered "stale" if it has had no activity (comments, updates) for the last quarter.
Stale PRs will be commented and labeled as such (using the `tag: stale` label), and then closed if they receive no update after a week.

Closed PRs can be reopened at any time, but they may be closed again if they meet the same stale conditions.
