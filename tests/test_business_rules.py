"""Lightweight tests for the core business-rule expectations."""
def is_valid_record(record):
    required = ("employee_id", "employee_name", "employer_id", "employer_name")
    if any(not str(record.get(key, "")).strip() for key in required):
        return False
    try:
        salary = float(record["annual_salary"])
        contribution = float(record["employer_contribution"])
    except (TypeError, ValueError, KeyError):
        return False
    return salary >= 0 and contribution >= 0 and bool(record.get("effective_date"))

def test_valid_record():
    record = {
        "employee_id": "E1", "employee_name": "Test Person",
        "employer_id": "ORG1", "employer_name": "Example Co",
        "annual_salary": "500000", "employer_contribution": "50000",
        "effective_date": "2026-04-01"
    }
    assert is_valid_record(record)

def test_negative_salary_rejected():
    record = {
        "employee_id": "E1", "employee_name": "Test Person",
        "employer_id": "ORG1", "employer_name": "Example Co",
        "annual_salary": "-1", "employer_contribution": "50000",
        "effective_date": "2026-04-01"
    }
    assert not is_valid_record(record)

def test_missing_employee_name_rejected():
    record = {
        "employee_id": "E1", "employee_name": "",
        "employer_id": "ORG1", "employer_name": "Example Co",
        "annual_salary": "500000", "employer_contribution": "50000",
        "effective_date": "2026-04-01"
    }
    assert not is_valid_record(record)
