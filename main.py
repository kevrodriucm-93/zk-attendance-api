import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import Json

app = FastAPI()

# CORS por si acaso
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
            "device_id": "TEST01",
            "DeviceID": "001",
            "sn": "SERIAL123",
        },
    ),
):
    # Validar token
    if token != ZK_TOKEN:
        raise HTTPException(status_code=403, detail="Token invalid")

    # Usuario
    user = str(
        body.get("user_id")
        or body.get("pin")
        or body.get("PIN")
        or "unknown"
    )

    # Fecha/hora
    raw_ts = (
        body.get("timestamp")
        or body.get("time")
        or body.get("LogTime")
    )
    try:
        ts = datetime.fromisoformat(str(raw_ts).replace("T", " ").split(".")[0])
    except Exception:
        ts = datetime.utcnow()

    # Código de dispositivo
    dispositivo_codigo = (
        str(body.get("device_id"))
        or str(body.get("DeviceID"))
        or str(body.get("sn"))
        or "DESCONOCIDO"
    )

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
            VALUES (%s, %s, %s, %s)
            """,
            (user, ts, dispositivo_codigo, Json(body)),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    print("ZK ENDPOINT /zk hit:", body)  # lo verás en logs de Render
    return {"ok": True}


# 👉 Catch-all para capturar lo que mande el reloj aunque use otra ruta
@app.post("/{catchall:path}")
async def zk_catch_all(
    catchall: str,
    request: Request,
):
    try:
        body = await request.json()
    except Exception:
        body = {"raw": "no-json-body"}

    print("CATCH_ALL HIT:", catchall, body)  # aparecerá en los logs de Render

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
            VALUES (%s, %s, %s, %s)
            """,
            (
                "unknown",
                datetime.utcnow(),
                catchall[:50],  # usamos el path como "dispositivo_codigo" provisional
                Json(body),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return {"ok": True, "path": catchall}
