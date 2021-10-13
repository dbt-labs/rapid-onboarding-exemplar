<!---
Provide a short summary in the Title above. Examples of good PR titles:
* "Feature: add so-and-so models"
* "Fix: deduplicate such-and-such"
* "Update: dbt version 0.14.0"
-->

## Description & motivation
<!---
Describe your changes, and why you're making them. Is this linked to an open
issue, a Trello card, or another pull request? Link it here.
-->


## Changes to existing models:
<!---
Include this section if you are changing any existing models. Link any related
pull requests on your BI tool, or instructions for merge (e.g. whether old
models should be dropped after merge, or whether a full-refresh run is required)
-->

## Screenshots (DAG, query results):
<!---
Include a screenshot of the relevant section of the updated DAG and if you've
created a new model, then show us the results when you query the model. To see
your version of the DAG, run `dbt docs generate && dbt docs serve`.
-->

## Validation of models:
<!---
All PRs should have a test criteria to confirm that the quality of their changes,
which should have been established in your task card. This might look like a
link to an in-development Looker dashboard or a query that compares an existing
model with a new one.
-->

## Checklist:
<!---
This checklist is mostly useful as a reminder of small things that can easily be
forgotten – it is meant as a helpful tool rather than hoops to jump through.
Put an `x` in all the items that apply, make notes next to any that haven't been
addressed, and remove any items that are not relevant to this PR.
-->
- [ ] My pull request represents one logical piece of work.
- [ ] My commits are related to the pull request and look clean.
- [ ] My SQL follows the [dbt Labs style guide](https://github.com/dbt-labs/corp/blob/master/dbt_style_guide.md).
- [ ] I have materialized my models appropriately.
- [ ] I have added appropriate tests and documentation to any new models or fields.
- [ ] I have noted if my changes break something in Looker and have a corresponding Looker PR to resolve the breaking change