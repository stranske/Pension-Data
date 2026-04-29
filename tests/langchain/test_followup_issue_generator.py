from scripts.langchain.followup_issue_generator import _select_followup_acceptance_criteria


def test_select_followup_acceptance_criteria_filters_workflow_sync_items_for_repo_local_db_work() -> (
    None
):
    acceptance = [
        "The follow-up PR does not add or modify Workflows-owned scripts in Pension-Data",
        "The file tests/test_database_strategy.py exists and contains test functions for staging_consultant_engagements",
        "At least one test verifies that after running migrations, the staging_consultant_engagements table exists in the database",
    ]
    concerns = [
        "Missing migration-path verification for staging_consultant_engagements table creation"
    ]

    selected = _select_followup_acceptance_criteria(acceptance, concerns)

    assert len(selected) == 2
    assert all("Workflows-owned scripts" not in item for item in selected)
    assert any("staging_consultant_engagements" in item for item in selected)


def test_select_followup_acceptance_criteria_keeps_workflow_items_when_not_repo_local() -> None:
    acceptance = [
        "The follow-up PR does not add or modify Workflows-owned scripts in Pension-Data",
        "The follow-up PR does not modify files under .github/workflows/",
    ]
    concerns = ["Address verifier metadata mismatch in generated issue text"]

    selected = _select_followup_acceptance_criteria(acceptance, concerns)

    assert selected == acceptance


def test_select_followup_acceptance_criteria_filters_workflow_items_by_acceptance_mix() -> None:
    acceptance = [
        "The follow-up PR does not add or modify Workflows-owned scripts in Pension-Data",
        "The file tests/test_database_strategy.py exists and contains test functions for staging_consultant_engagements",
    ]
    concerns = ["Clarify issue text in follow-up summary"]

    selected = _select_followup_acceptance_criteria(acceptance, concerns)

    assert selected == [
        "The file tests/test_database_strategy.py exists and contains test functions for staging_consultant_engagements"
    ]
