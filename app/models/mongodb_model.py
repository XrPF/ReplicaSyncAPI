from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime
from bson import ObjectId, Decimal128

class MongoDBModel(BaseModel):
    _id: Optional[str]
    a: str
    aname: str
    aid: int
    c: str
    cname: str
    d: datetime
    u: int
    expectedLoss: str
    financialData: str
    status: int
    comment: Optional[str]
    Application: str
    LineOfBusiness: str

    @validator('_id', pre=True, always=True)
    def validate_id(cls, v):
        return str(v)

    @validator('expectedLoss', pre=True, always=True)
    def validate_expected_loss(cls, v):
        return str(v)