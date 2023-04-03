import queue
import threading
from preparaTabelas import PreparaTabelas
from processaNotas import ProcessaNotas
from db import Database


class ProcessaNotasThread(threading):
    def __init__(self, db, item):
        self.db = db
        self.item = item

    def run(self):
        pn = ProcessaNotas(self.db)
        pn.processaItems(self.item)
