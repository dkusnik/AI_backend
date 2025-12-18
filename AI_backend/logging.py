import logging


class SafeExtraFilter(logging.Filter):
    def filter(self, record):
        for attr in ("task_id", "delivery_uuid"):
            if not hasattr(record, attr):
                setattr(record, attr, "-")
        return True