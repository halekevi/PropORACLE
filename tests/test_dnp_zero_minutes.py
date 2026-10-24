from utils.dnp_zero_minutes import minutes_status


def test_zero_minutes_are_dnp_and_blank_stays_unknown():
    assert minutes_status(0) == "dnp"
    assert minutes_status(None) == "minutes_unknown"
    assert minutes_status(12.5) == "played"
