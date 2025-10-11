from pydantic import BaseModel, HttpUrl, Field
# from typing import Optional

class LeagueInjuriesRequest(BaseModel):
    url: HttpUrl
    # season: Optional[str] = None
    # Future: allow pagination if you want
    # maxPages: int = Field(1, ge=1, le=10)
