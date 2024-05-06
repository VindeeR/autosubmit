def custom_return_value(value=None):
    """
    Generates a dummy function that always return the given 'value'.\n
    Useful for using it with `monkeypatch.setattr(...)`
    """

    def blank_func(*args, **kwargs):
        return value

    return blank_func
