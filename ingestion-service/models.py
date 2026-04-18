from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class ReviewContract(BaseModel):
    review_id: str = Field(..., min_length=22, max_length=22)
    user_id: str = Field(..., min_length=22, max_length=22)
    business_id: str = Field(..., min_length=22, max_length=22)
    stars: float = Field(..., ge=0, le=5)
    useful: int = Field(default=0, ge=0)
    funny: int = Field(default=0, ge=0)
    cool: int = Field(default=0, ge=0)
    text: Optional[str] = None
    date: datetime

    @field_validator('text')
    @classmethod
    def text_must_not_be_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and len(v.strip()) == 0:
            raise ValueError('Review text cannot be empty')
        return v