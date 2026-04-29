from scripts.langchain.followup_issue_generator import (
    OriginalIssueData,
    VerificationData,
    _build_why_section,
    _select_followup_acceptance_criteria,
)


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


def test_build_why_section_emphasizes_repo_local_summary_for_mixed_surface() -> None:
    verification = VerificationData(
        concerns=["Missing migration-path verification for staging_consultant_engagements"],
        tasks_attempted=1,
        tasks_completed=0,
        iteration_count=1,
    )
    issue = OriginalIssueData(
        number=339,
        acceptance_criteria=[
            "The follow-up PR does not add or modify Workflows-owned scripts in Pension-Data",
            "At least one test verifies that after running migrations, the staging_consultant_engagements table exists in the database",
        ],
    )

    why = _build_why_section(verification, issue, pr_number=362, verdict="FAIL")

    assert "repo-local" in why
    assert "migration and database-test evidence" in why
    assert "workflow-sync criteria" in why
