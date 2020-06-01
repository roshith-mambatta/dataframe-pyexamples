from dataclasses import dataclass

@dataclass(frozen=True)
class Employee(object):
    firstName: str
    lastName: str
    age: int
    weightInLbs: float
    jobType: str
