import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Body
import psycopg2
from psycopg2.extras import Json

app = FastAPI()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
ZK_TOKEN = os.getenv("ZK_TOKEN", "mytoken")  # valor por defecto

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

@app.post("/zk/{token}")
async def receive_zk(
    token: str,
    body: dict = Body(
        ...,
        example={
            "user_id": "123",
            "timestamp": "2025-12-08 14:30:00",
            "device_id": "TEST01"
        },
    ),
):
    # Validar token
    if token != ZK_TOKEN:
        raise HTTPException(status_code=403, detail="Token invalid")

    # Extraer datos básicos
    user = str(body.get("user_id") or body.get("pin") or "unknown")
    raw_ts = body.get("timestamp") or body.get("time") or body.get("LogTime")

    try:
        ts = datetime.fromisoformat(str(raw_ts).replace("T", " ").split(".")[0])
    except Exception:
        ts = datetime.utcnow()

    # Guardar en BD
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO marcajes (zk_user_id, fecha_hora, bruto_json)
            VALUES (%s, %s, %s)
            """,
            (user, ts, Json(body)),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        # para depurar si algo va mal con la BD
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return {"ok": True}
