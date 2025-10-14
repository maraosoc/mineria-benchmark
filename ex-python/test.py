from common.benchmark_utils import extract_status

def test_extract_status():
    assert extract_status('HTTP Status Code: 200') == '200'
