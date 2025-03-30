from fastapi import APIRouter, Depends, Request

router = APIRouter()

async def get_mom(request: Request):
    return request.app.state.mom

async def get_dad(request: Request):
    return request.app.state.dad

@router.get("/pool")
async def pool(mom = Depends(get_mom), dad = Depends(get_dad)):
    mom = await mom.get("last")
    dad = await dad.get("last")
    return {"mom": mom, "dad": dad}