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


# ============================================================
#  Endpoint de pruebas manuales con JSON (/zk/{token})
#  (NO lo usa el reloj, solo Postman / Swagger)
# ============================================================
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
        # aquí no usamos sn/tipo/etc. porque es solo para pruebas
        cur.execute(
            """
            INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (zk_user_id, fecha_hora, dispositivo_codigo) DO NOTHING
            """,
            (user, ts, dispositivo_codigo, Json(body)),
        )
        conn.commit()
        
    except Exception as e:
        try:
            # Insertar en log de errores
            cur.execute(
                """
                INSERT INTO marcajes_error_log 
                (error_message, zk_user_id, fecha_hora, dispositivo_codigo, bruto_json, original_body, stack_trace)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(e),
                    user,
                    ts,
                    dispositivo_codigo,
                    Json(body),  # JSON estructurado
                    json.dumps(body),  # Texto plano para debugging
                    traceback.format_exc()
                )
            )
            conn.commit()
        except Exception as log_error:
            # Si falla el log, al menos registrar en consola/archivo
            print(f"Error crítico - No se pudo loguear: {log_error}")
            print(f"Original error: {e}")
            print(f"Original body: {json.dumps(body)}")
       
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

    print("ZK JSON ENDPOINT /zk HIT:", body)
    return {"ok": True}


# ============================================================
#  Endpoint oficial ADMS ZKTeco: /iclock/cdata
#  - GET: handshake
#  - POST: marcajes ATTLOG, OPLOG, DEVINFO
# ============================================================
@app.api_route("/iclock/cdata", methods=["GET", "POST"])
async def iclock_cdata(request: Request):
    """
    Modo CAJA NEGRA:
    - GET: guardamos handshake con query params.
    - POST: guardamos TODO el body crudo + query params.
    No se intenta parsear ATTLOG/OPLOG/DEVINFO.
    """
    now = datetime.utcnow()
    params = dict(request.query_params)
    sn = params.get("SN") or params.get("sn") or "UNKNOWN_SN"
    table = params.get("table") or "UNKNOWN"

    try:
        conn = get_conn()
        cur = conn.cursor()

        if request.method == "GET":
            # Handshake del dispositivo
            payload = {
                "method": "GET",
                "query": params,
            }
            cur.execute(
                """
                INSERT INTO marcajes (
                    zk_user_id, fecha_hora, dispositivo_codigo,
                    sn, tipo, method, bruto_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "HANDSHAKE",    # zk_user_id
                    now,            # fecha_hora
                    sn,             # dispositivo_codigo
                    sn,             # sn
                    "HANDSHAKE",    # tipo
                    "GET",          # method
                    Json(payload),
                ),
            )
        else:
            # POST -> recibimos payload crudo (ATTLOG, OPERLOG, lo que sea)
            raw_bytes = await request.body()
            raw_text = raw_bytes.decode("utf-8", errors="ignore") if raw_bytes else ""

            payload = {
                "method": "POST",
                "query": params,
                "raw": raw_text,
            }

            # IMPORTANTE:
            # - zk_user_id lo dejamos NULL para no violar el unique
            #   (en Postgres, NULL en UNIQUE no choca entre filas).
            # - tipo lo marcamos con el table=... si viene, o UNKNOWN.
            cur.execute(
                """
                INSERT INTO marcajes (
                    zk_user_id, fecha_hora, dispositivo_codigo,
                    sn, tipo, method, bruto_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    None,          # zk_user_id (NULL → no choca en UNIQUE)
                    now,           # fecha_hora (momento de recepción)
                    sn,            # dispositivo_codigo
                    sn,            # sn
                    table,         # tipo (ATTLOG, OPERLOG, etc., si viene)
                    "POST",        # method
                    Json(payload),
                ),
            )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        # Log para ver el error si algo va mal
        print("ERROR en /iclock/cdata:", e)
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    # ZKTeco espera texto plano "OK"
    return PlainTextResponse("OK")



# ============================================================
#  Endpoints para comandos (de momento sin comandos)
#  /iclock/getrequest y /iclock/devicecmd
# ============================================================
@app.get("/iclock/getrequest")
async def iclock_getrequest(request: Request):
    now = datetime.utcnow()
    params = dict(request.query_params)
    sn = params.get("SN") or params.get("sn") or "UNKNOWN_SN"

    # opcional: loguear que el equipo pidió comandos
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO marcajes (
                zk_user_id, fecha_hora, dispositivo_codigo,
                sn, tipo, method, bruto_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            ("GETREQUEST", now, sn, sn, "GETREQUEST", "GET", Json({"query": params})),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        # si falla BD, no bloqueamos la comunicación
        pass

    # Por ahora no enviamos comandos, así que respondemos vacío
    return PlainTextResponse("")


@app.get("/iclock/devicecmd")
async def iclock_devicecmd(request: Request):
    # algunos firmwares consultan aquí en vez de /iclock/getrequest
    return await iclock_getrequest(request)
