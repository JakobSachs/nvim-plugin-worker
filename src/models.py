import uuid
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl, field_serializer

from pydantic_core import Url


class Repository(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    readme: str = Field(default="")
    name: str = Field(...)
    author: str = Field(...)
    url: HttpUrl = Field(...)
    description: str = Field(...)
    stars: int = Field(default=0)
    language: str = Field(default="")
    last_updated: datetime = Field(default=datetime.now())

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example/repo": {
                "name": "repo",
                "url": "http://example.com",
                "description": "This is an example repo",
                "stars": 0,
            }
        }

    @field_serializer("url")
    def format_url(self, v):
        if isinstance(v, Url):
            return str(v)
        return v
