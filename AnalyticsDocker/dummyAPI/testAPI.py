from fastapi import FastAPI
from typing import Union
import random

from datetime import datetime, timedelta


import logging
import sys

logger = logging.getLogger(__name__)


class Items:
    
    def __init__(self):
        # Initialize the dictionary with dummy data
        self.items = {
            '1': {'name': 'Sifon Simple', 'price': 1200, 'family': 'Family A', 'cicleTime': 75},
            '2': {'name': 'Sifon PVC', 'price': 800, 'family': 'Family A', 'cicleTime': 40},
            '3': {'name': 'Sifon Doble', 'price': 500, 'family': 'Family B', 'cicleTime': 90},
        }

    def getRandomItem(self):
        # Select a random item key
        random_key = random.choice(list(self.items.keys()))
        return random_key
    
    def getAllItems(self):
        return self.items
    

class Customer:
    
    def __init__(self):
        # Initialize the dictionary with dummy data
        self.customer = {
            '1': {'name': 'ACME', 'country': 'Argentina'},
            '2': {'name': 'Arsat', 'country': 'Argentina'},
            '3': {'name': 'AFIP', 'country': 'Brazil'},
            '4': {'name': 'NASA', 'country': 'Brazil'},
            '5': {'name': 'Tesla', 'country': 'Uruguay'},
            '6': {'name': 'Google', 'country': 'Uruguay'},
            '7': {'name': 'Amazon', 'country': 'Argentina'},
            '8': {'name': 'Mercadolibre', 'country': 'Argentina'},
        }

    def getRandomCust(self):
        # Select a random customer key
        random_key = random.choice(list(self.customer.keys()))
        return random_key
    
    def getAllCust(self):
        return self.customer


class SO:

    def __init__(self, fromDate):
        self.fromDate = fromDate

    def calculateSOStatus(self, qty, qtyFullfilled, qtyShipped):

        if qtyFullfilled == 0:
            return 'Approved'
                
        elif qtyShipped>0 and qtyShipped<qty:
            return 'Partially Shipped'
        
        elif qtyFullfilled > 0 and qtyShipped==0:
            return 'Partially Fulfilled'
        
        elif qtyFullfilled == qty and qtyShipped==0:
            return 'Fulfilled'
        
        else:
            return 'Shipped'

    def createSO(self):
        
        cusInstance = Customer()
        randomCus = cusInstance.getRandomCust()

        itemsInstance = Items()
        randomItem = itemsInstance.getRandomItem()      

        soDate = self.fromDate + timedelta(days = random.randint(0, 5))
        soDueDate = soDate + timedelta(days = random.randint(0, 15))

        qty = random.randint(1, 5)
        # I specify values and their weight in order to give more probability to finished status
        choices = [0, 1, 2, 3, 4, 5]
        weights = [0.3, 0.1, 0.1, 0.1, 0.1, 0.3] 
        qtyFullfilled = random.choices(choices[:qty+1], weights[:qty+1])[0]
        qtyShipped = random.choices(choices[:qtyFullfilled+1], weights[:qtyFullfilled+1])[0]

        # Depending the values randomly generated the SO Status is calculated to match the values
        soStatus = self.calculateSOStatus(qty, qtyFullfilled, qtyShipped)

        if soStatus == 'Shipped':
            shipDate = soDate + timedelta(days = random.randint(0, 15))
        else:
            shipDate = None

        soDict = {
            'idCustomer' : randomCus,
            'idItem' : randomItem,
            'createdDate' : soDate,
            'dueDate' : soDueDate,
            'shipDate' : shipDate,
            'qty' : qty,
            'qtyFullfilled' : qtyFullfilled,
            'qtyShipped' : qtyShipped,
            'soStatus' : soStatus
        }
        
        return soDict

class WO:
    
    def __init__(self, idSO, idItem, fromDate, qtyFullfilled):
        self.idSO = idSO
        self.idItem = idItem
        self.fromDate = fromDate
        self.qtyFullfilled = qtyFullfilled

    def createWO(self):
        
        # I specify values and their weight in order to give more probability to no scrap
        choices = [0, 1, 2, 3, 4, 5]
        weights = [0.5, 0.1, 0.1, 0.1, 0.1, 0.1] 
        qtyScrap = random.choices(choices, weights)[0]

        woDate = self.fromDate + timedelta(days = random.randint(0, 5))

        soDict = {
            'idSO' : self.idSO,
            'idItem' : self.idItem,
            'createdDate' : woDate,
            'qty' : self.qtyFullfilled,
            'qtyScrap' : qtyScrap,
        }
        
        return soDict


app = FastAPI()


@app.get("/soData")
def soData(paramDate):
    
    # http://127.0.0.1:8000/soData?paramDate=2024-01-01

    frmtDate = datetime.strptime(paramDate, "%Y-%m-%d")

    soInstance = SO(fromDate=frmtDate)
    
    soData = soInstance.createSO()
    
    return soData


@app.get("/woData")
def woData(idSO, idItem, fromDate, qtyFullfilled):
    
    # http://127.0.0.1:8000/woData?idSO=1&idItem=1&fromDate=2024-01-01&qtyFullfilled=2

    frmtDate = datetime.strptime(fromDate, "%Y-%m-%d")

    woInstance = WO(idSO, idItem, frmtDate, qtyFullfilled)
    
    woData = woInstance.createWO()
    
    return woData


@app.get("/items")
def allItems():
    
    # http://127.0.0.1:8000/items

    itemsInstance = Items()
    
    allItems = itemsInstance.getAllItems()
    
    return allItems


@app.get("/customers")
def allCust():

    # http://127.0.0.1:8000/customers
    
    cusInstance = Customer()
    
    allCust = cusInstance.getAllCust()
    
    return allCust


import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)