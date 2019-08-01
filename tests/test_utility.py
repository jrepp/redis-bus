from redisbus.utility import DictObj


def test_dictobj():
    subject_dict = {
        "foo": "bar",
        "left": "right"
    }

    subject_obj = DictObj(subject_dict)

    assert hasattr(subject_obj, 'foo')
    assert hasattr(subject_obj, 'left')
    assert subject_obj.foo == "bar"
    assert subject_obj.left == "right"
