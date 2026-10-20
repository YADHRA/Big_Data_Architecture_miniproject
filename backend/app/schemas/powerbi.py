from pydantic import BaseModel


class PowerBIConfig(BaseModel):
    enabled: bool
    configured: bool
    message: str
    report_id: str | None = None
    embed_url: str | None = None
