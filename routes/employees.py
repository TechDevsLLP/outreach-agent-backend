from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import Optional
from auth import get_account_context
from services.employee_selection_service import select_and_enrich_employees

router = APIRouter(prefix="/api/employees", tags=["employees"])


class SelectAndEnrichRequest(BaseModel):
    employee_ids: list[str] = Field(..., min_length=1, max_length=100)
    auto_enrich: bool = True
    skip_email_finding: bool = False
    skip_profile_scrape: bool = False
    skip_company_scrape: bool = False
    skip_ai_assessment: bool = False
    skip_outreach: bool = False
    industry_id: Optional[str] = None
    tags: Optional[list[str]] = None


@router.post("/select-and-enrich")
async def select_and_enrich(
    body: SelectAndEnrichRequest,
    account_ctx=Depends(get_account_context),
):
    account_id = str(account_ctx["account"]["_id"])
    result = await select_and_enrich_employees(
        employee_ids=body.employee_ids,
        auto_enrich=body.auto_enrich,
        skip_email_finding=body.skip_email_finding,
        skip_profile_scrape=body.skip_profile_scrape,
        skip_company_scrape=body.skip_company_scrape,
        skip_ai_assessment=body.skip_ai_assessment,
        skip_outreach=body.skip_outreach,
        industry_id=body.industry_id,
        tags=body.tags,
        account_id=account_id,
    )
    return result
