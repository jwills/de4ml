from pydantic import BaseModel


class AgrawalRequest(BaseModel):
    salary: float
    commission: float
    age: int
    elevel: int
    car: int
    zipcode: int
    hvalue: int
    hyears: int
    loan: float
