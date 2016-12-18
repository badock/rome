__author__ = 'jonathan'

SECONDARY_INDEXES = {}


class SecondaryIndexDecorator(object):

    def __init__(self, attribute):
        self.attribute = attribute

    def __call__(self, model_class):
        current_secondary_indexes = getattr(model_class, "_secondary_indexes", [])
        setattr(model_class, "_secondary_indexes", current_secondary_indexes + [self.attribute])
        return model_class


def secondary_index_decorator(attribute):
    """
    Return an instance of 'SecondaryIndexDecorator'.
    :param attribute: an attribute name
    :return: an instance of 'SecondaryIndexDecorator'
    """
    return SecondaryIndexDecorator(attribute)
