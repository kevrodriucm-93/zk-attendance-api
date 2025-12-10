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
    now = datetime.utcnow()
    params = dict(request.query_params)
    sn = params.get("SN") or params.get("sn") or "UNKNOWN_SN"

    try:
        conn = get_conn()
        cur = conn.cursor()

        if request.method == "GET":
            # handshake
            data = {"method": "GET", "query": params}
            cur.execute(
                """
                INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                VALUES (%s, %s, %s, %s)
                """,
                ("HANDSHAKE", now, sn, Json(data)),
            )
            print("ICLOCK GET:", params)

        else:  # POST
            raw_bytes = await request.body()
            raw_text = raw_bytes.decode("utf-8", errors="ignore") if raw_bytes else ""
            print("ICLOCK POST RAW:", raw_text)

            lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]

            for line in lines:
                parts = line.split("\t")
                if not parts:
                    continue

                first = parts[0]

                # --- DEVINFO (~DeviceName=...) ---
                if first.startswith("~"):
                    data = {
                        "method": "POST",
                        "tipo": "DEVINFO",
                        "raw": line,
                    }
                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                        VALUES (%s, %s, %s, %s)
                        """,
                        ("DEVINFO", now, sn, Json(data)),
                    )
                    continue

                # --- OPLOG ... ---
                if first.startswith("OPLOG"):
                    data = {
                        "method": "POST",
                        "tipo": "OPLOG",
                        "raw": line,
                    }
                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                        VALUES (%s, %s, %s, %s)
                        """,
                        ("OPLOG", now, sn, Json(data)),
                    )
                    continue

                # --- ATTLOG (marcajes) ---
                # PIN \t Time \t Verified \t Status \t WorkCode...
                if len(parts) >= 2:
                    pin = parts[0].strip()
                    time_str = parts[1].strip()
                    verified = parts[2].strip() if len(parts) > 2 else None
                    status = parts[3].strip() if len(parts) > 3 else None
                    workcode = parts[4].strip() if len(parts) > 4 else None

                    try:
                        ts = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        ts = now

                    data = {
                        "method": "POST",
                        "tipo": "ATTLOG",
                        "raw": line,
                        "pin": pin,
                        "time": time_str,
                        "verified": verified,
                        "status": status,
                        "workcode": workcode,
                        "sn": sn,
                    }

                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (pin, ts, sn, Json(data)),
                    )
                else:
                    # línea rara, la guardamos tal cual
                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
                        VALUES (%s, %s, %s, %s)
                        """,
                        ("UNKNOWN", now, sn, Json({"raw": line})),
                    )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return PlainTextResponse("OK")