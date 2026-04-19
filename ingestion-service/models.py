from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

class ReviewContract(BaseModel):
    
    review_id: str
    user_id: str
    business_id: str
    stars: float = Field(ge=0, le=5)
    useful: int = Field(ge=0)
    funny: int = Field(ge=0)
    cool: int = Field(ge=0)
    text: str
    date: datetime

    @field_validator('date', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
        return v

class BusinessContract(BaseModel):
    
    business_id: str
    name: str
    address: Optional[str] = ""
    city: str
    state: str
    postal_code: Optional[str] = ""
    latitude: float
    longitude: float
    stars: float = Field(ge=0, le=5)
    review_count: int = Field(ge=0)
    is_open: int = Field(ge=0, le=1)
    categories: Optional[str] = "Uncategorized"

class UserContract(BaseModel):
    
    user_id: str
    name: str
    review_count: int = Field(ge=0)
    yelping_since: datetime
    useful: int = Field(ge=0)
    funny: int = Field(ge=0)
    cool: int = Field(ge=0)
    fans: int = Field(ge=0)
    average_stars: float = Field(ge=0, le=5)

    @field_validator('yelping_since', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
        return v