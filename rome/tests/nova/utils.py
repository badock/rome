def clean_rome_database(model_classes):
    from rome.core.session.session import Session
    session = Session()
    for model_class in model_classes:
        for element in session.query(model_class):
            session.delete(element)
    session.flush()


def clean_rome_all_databases():
    models_classes = []
    import models as models_module
    import inspect
    for name, obj in inspect.getmembers(models_module):
        if inspect.isclass(obj) and hasattr(obj, "_sa_class_manager"):
            models_classes += [obj]
    clean_rome_database(models_classes)