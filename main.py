import os, httpx, asyncio, json
from dotenv import load_dotenv
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone

load_dotenv()
a = FastAPI()
c = AsyncIOMotorClient(os.getenv("URL"))[os.getenv("NAME")]
b = int(os.getenv("SYNC_BATCH_SIZE", 10))

CL, S, W, P = os.getenv("DB_COL"), os.getenv("DB_FLD_S"), os.getenv("DB_FLD_W"), os.getenv("DB_FLD_P")
M1, M2, M3, M4, M5 = os.getenv("MSG_D_ERR"), os.getenv("MSG_H_ERR"), os.getenv("MSG_DB_ERR"), os.getenv("MSG_OK"), os.getenv("MSG_EMPTY")

@a.get("/api/v1/sync-msg-logs")
async def s():
    try:
        r = await c[CL].find({S: False}).sort("_id", 1).to_list(b)
        if not r: return {"st": M5}
        async with httpx.AsyncClient() as cl:
            for i in r:
                u, py = i.get(W), i.get(P)
                if u and py:
                    try:
                        res = await cl.post(u, json=py)
                        if res.status_code < 400:
                            await c[CL].update_one(
                                {"_id": i["_id"]}, 
                                {"$set": {S: True, "f_e": datetime.now(timezone.utc)}}
                            )
                        else: 
                            return {"st": M1, "c": res.status_code, "id": str(i["_id"])}
                    except Exception as e: 
                        return {"st": M2, "m": str(e)}
        return {"st": M4, "n": len(r)}
    except Exception as e: 
        return {"st": M3, "m": str(e)}
    
if __name__ == "__main__":
    import asyncio
    asyncio.run(s())