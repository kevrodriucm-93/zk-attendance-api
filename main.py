import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
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


# ---- Endpoint de pruebas manuales con JSON ----
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
    if token != ZK_TOKEN:
        raise HTTPException(status_code=403, detail="Token invalid")

    user = str(
        body.get("user_id")
        or body.get("pin")
        or body.get("PIN")
        or "unknown"
    )

    raw_ts = (
        body.get("timestamp")
        or body.get("time")
        or body.get("LogTime")
    )
    try:
        ts = datetime.fromisoformat(str(raw_ts).replace("T", " ").split(".")[0])
    except Exception:
        ts = datetime.utcnow()

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

    print("ZK JSON ENDPOINT /zk HIT:", body)
    return {"ok": True}


# ---- Endpoint oficial ADMS ZKTeco ----
@app.api_route("/iclock/cdata", methods=["GET", "POST"])
async def iclock_cdata(request: Request):
    """
    Implementación mínima del protocolo ADMS:
    - GET: handshake del dispositivo -> respondemos 'OK'
    - POST: envío de marcajes en texto plano -> guardamos raw y respondemos 'OK'
    """
    now = datetime.utcnow()

    try:
        conn = get_conn()
        cur = conn.cursor()

        if request.method == "GET":
            # Guardamos el handshake con los query params
            params = dict(request.query_params)
            data = {"method": "GET", "query": params}
            cur.execute(
                """
                INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                VALUES (%s, %s, %s, %s)
                """,
                ("iclock_get", now, "iclock/cdata", Json(data)),
            )
            print("ICLOCK GET:", params)

        else:  # POST
            raw_bytes = await request.body()
            raw_text = raw_bytes.decode("utf-8", errors="ignore") if raw_bytes else ""
            data = {"method": "POST", "raw": raw_text}
            cur.execute(
                """
                INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                VALUES (%s, %s, %s, %s)
                """,
                ("iclock_post", now, "iclock/cdata", Json(data)),
            )
            print("ICLOCK POST RAW:", raw_text)

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    # ZKTeco espera texto plano 'OK'
    return PlainTextResponse("OK")
