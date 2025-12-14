
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, HttpUrl, Field, field_validator, model_validator


class RSSArticle(BaseModel):
    # Identité / provenance
    source: str = Field(..., min_length=2, max_length=80)
    category: str = Field(..., min_length=1, max_length=80)

    # Contenu
    title: str = Field(..., min_length=5, max_length=500)
    content: Optional[str] = Field(default=None, max_length=20000)

    # Lien & temps
    url: HttpUrl
    published_at: datetime

    # Langue
    lang: str = Field(..., min_length=2, max_length=8)

    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v: str) -> str:
        vv = v.strip()
        if len(vv) < 5:
            raise ValueError("Title too short")
        return vv[:500]

    @field_validator("content")
    @classmethod
    def normalize_content(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = v.strip()
        return vv[:20000] if vv else None

    @field_validator("lang")
    @classmethod
    def normalize_lang(cls, v: str) -> str:
        vv = v.strip().lower()
        # Normalisation simple (tu peux enrichir plus tard)
        if vv in ("fr-fr", "français"):
            return "fr"
        if vv in ("en-us", "english"):
            return "en"
        if vv in ("ar-sa", "arabic"):
            return "ar"
        if len(vv) < 2:
            raise ValueError("Invalid lang")
        return vv[:8]

    @field_validator("published_at")
    @classmethod
    def ensure_datetime_is_tzaware(cls, v: datetime) -> datetime:
        # Si RSS donne une datetime naive, on la force en UTC (simple et stable)
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    @model_validator(mode="after")
    def basic_consistency(self) -> "RSSArticle":
        # Ex: éviter category vide
        if not self.category.strip():
            raise ValueError("Category empty")
        return self
