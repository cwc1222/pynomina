import logging


class Logger:
    def __init__(self):
        super().__init__()
        logging.basicConfig(level=logging.INFO)

    def getLogger(self, class_name: str) -> logging.Logger:
        return logging.getLogger(class_name)
