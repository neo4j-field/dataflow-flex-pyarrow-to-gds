from . import util

def test_strtobool():
    """Rather silly unit test here...should be generative."""
    tests = [
        ('yes', 1),
        ('Yes', 1),
        ('Y', 1),
        ('T', 1),
        ('OFF', 0),
        ('0', 0),
    ]
    for test in tests:
        assert util.strtobool(test[0]) == test[1]
