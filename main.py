import requests
import time
import re

# API authentication and configuration
API_KEY = 'ak_35701fb94867a7c5630369c9a9a702e972680ecd31568e5b'
BASE_URL = 'https://assessment.ksensetech.com/api'
HEADERS = {"x-api-key": API_KEY}
MAX_RETRIES = 10

# Function to fetch all patients data from paginated API
def get_patients():
    all_patients = []
    page = 1
    while True:
        for attempt in range(MAX_RETRIES):
            try:
                # Fetch a page of patient data
                res = requests.get(f"{BASE_URL}/patients?page={page}&limit=5", headers=HEADERS)
                if res.status_code == 200:
                    data = res.json()
                    all_patients.extend(data.get("data", [])) # Add patients from current page
                    
                    # Check if more pages exists
                    if data.get("pagination", {}).get("hasNext", False):
                        page += 1
                        break  # Move to next page
                    else:
                        return all_patients # No more pages, return result
                    
                # Handle rate limting
                elif res.status_code == 429:
                    time.sleep(2 ** attempt)  # Exponential backoff

                # Handle server errors
                elif res.status_code in [500, 503]:
                    time.sleep(1) 
                else:
                    break  # Unexpected status, stop retrying this page

            except requests.exceptions.RequestException:
                time.sleep(2) # Handle network issues and retry

def parse_blood_pressure(bp):
    """
    Parse blood pressure string and return a risk score as per rules.
    Returns:
    - risk score (1 to 4)
    - or 0 if invalid/missing format
    """
    if not bp or not isinstance(bp, str):
        return 0
    
    bp = bp.strip()
    match = re.match(r"^(\d{2,3})/(\d{2,3})$", bp)
    if not match:
        return 0
    try:
        systolic = int(match.group(1))
        diastolic = int(match.group(2))
    except:
        return 0
    
    # Determine systolic stage
    if systolic < 120:
        systolic_stage = 1
    elif 120 <= systolic <= 129:
        systolic_stage = 2
    elif 130 <= systolic <= 139:
        systolic_stage = 3
    elif systolic >= 140:
        systolic_stage = 4
    else:
        systolic_stage = 0  # Should not occur, fallback

    # Determine diastolic stage
    if diastolic < 80:
        diastolic_stage = 1
    elif 80 <= diastolic <= 89:
        diastolic_stage = 3
    elif diastolic >= 90:
        diastolic_stage = 4
    else:
        diastolic_stage = 0  # Should not occur, fallback

    # Return higher risk stage
    return max(systolic_stage, diastolic_stage)

def parse_temperature(temp):
    """
    Parses temperature.
    Returns:
    - 0 (normal ≤ 99.5)
    - 1 (low fever 99.6–100.9)
    - 2 (high fever ≥ 101)
    - -1 for invalid/missing data.
    """
    try:
        temp_f = float(temp)
        if temp_f <= 99.5:
            return 0
        elif 99.6 <= temp_f <= 100.9:
            return 1
        elif temp_f >= 101.0:
            return 2
        else:
            return 0
    except:
        return -1

def parse_age(age):
    """
    Parses age and returns risk score:
    - 1 point if age < 40 or age between 40-65 inclusive
    - 2 points if age > 65
    - 0 for invalid/missing data
    """
    try:
        age_int = int(age)
        if age_int < 0:
            return 0
        if age_int > 65:
            return 2
        else:
            return 1
    except:
        return 0

def valid_bp_format(bp):
    if not bp or not isinstance(bp, str):
        return False
    return bool(re.match(r"^\d{2,3}/\d{2,3}$", bp.strip()))

def is_valid_age(age):
    try:
        age_int = int(age)
        return age_int >= 0
    except:
        return False

def is_valid_temperature(temp):
    try:
        float(temp)
        return True
    except:
        return False

def analyze_patients(patients):
    high_risk = []
    fever = []
    quality_issues = []

    for p in patients:
        pid = p.get("patient_id", "").strip()
        bp_val = p.get("blood_pressure", "")
        temp_val = p.get("temperature", "")
        age_val = p.get("age", "")

        bp_score = parse_blood_pressure(bp_val)
        temp_score = parse_temperature(temp_val)
        age_score = parse_age(age_val)

        # Data quality checks
        bp_invalid = (bp_score == 0 and not valid_bp_format(bp_val))
        temp_invalid = (temp_score == -1)
        age_invalid = (age_score == 0 and not is_valid_age(age_val))

        if bp_invalid or temp_invalid or age_invalid:
            quality_issues.append(pid)

        # Only if all data is valid, evaluate risk and fever
        if not (bp_invalid or temp_invalid or age_invalid):
            total_risk = bp_score + temp_score + age_score
            if total_risk >= 4:
                high_risk.append(pid)
            
            # Fever check: temp >= 99.6°F
            try:
                temp_f = float(temp_val)
                if temp_f >= 99.6:
                    fever.append(pid)
            except:
                pass
    
    # Remove duplicates
    high_risk = list(set(high_risk))
    fever = list(set(fever))
    quality_issues = list(set(quality_issues))

    return high_risk, fever, quality_issues


# Submit results
def submit_results(high_risk, fever, quality_issues):
    result = {
        "high_risk_patients": high_risk,
        "fever_patients": fever,
        "data_quality_issues": quality_issues
    }
    print("Response:", result)
    res = requests.post(
        f"{BASE_URL}/submit-assessment",
        headers={**HEADERS, "Content-Type": "application/json"},
        json=result
    )
    print("Submission Response:", res.json())


patients = get_patients()
high_risk, fever, quality_issues = analyze_patients(patients)
submit_results(high_risk, fever, quality_issues)