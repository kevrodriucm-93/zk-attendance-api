import logging
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
            INSERT INTO logs (zk_user_id, fecha_hora, dispositivo_codigo, bruto_json)
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
    return {"OK": True}


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
                INSERT INTO logs (
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
            lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]

            for line in lines:
                parts = line.split("\t")
                if not parts:
                    continue

                first = parts[0]

                # --- DEVINFO (~DeviceName=...) ---
                if first.startswith("~"):
                    data = {
                        "sn": sn,
                        "raw": line,
                        "tipo": "DEVINFO",
                        "method": "POST",
                    }
                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo,
                                              sn, tipo, method, bruto_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("DEVINFO", now, sn, sn, "DEVINFO", "POST", Json(data)),
                    )
                    continue

                # --- OPLOG ... ---
                if first.startswith("OPLOG"):
                    data = {
                        "sn": sn,
                        "raw": line,
                        "tipo": "OPLOG",
                        "method": "POST",
                    }
                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo,
                                              sn, tipo, method, bruto_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("OPLOG", now, sn, sn, "OPLOG", "POST", Json(data)),
                    )
                    continue

                # --- ATTLOG (marcajes) ---
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
                        "sn": sn,
                        "pin": pin,
                        "raw": line,
                        "time": time_str,
                        "tipo": "ATTLOG",
                        "method": "POST",
                        "status": status,
                        "verified": verified,
                        "workcode": workcode,
                    }

                    cur.execute(
                        """
                        INSERT INTO marcajes (
                            zk_user_id, fecha_hora, dispositivo_codigo,
                            sn, tipo, method, verified, status, workcode, bruto_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            pin,
                            ts,
                            sn,
                            sn,
                            "ATTLOG",
                            "POST",
                            verified,
                            status,
                            workcode,
                            Json(data),
                        ),
                    )
                    print("✅ Marcaje guardado exitosamente")
                else:
                    cur.execute(
                        """
                        INSERT INTO marcajes (zk_user_id, fecha_hora, dispositivo_codigo,
                                              sn, tipo, method, bruto_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("UNKNOWN", now, sn, sn, "UNKNOWN", "POST", Json({"raw": line})),
                    )
                    print("⚠️ Revisar body en marcajes")

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
                    "UNKNOWN",
                    now,
                    sn,
                    None,  # JSON estructurado
                    raw_text,  # Texto plano para debugging
                    traceback.format_exc()
                )
            )
            conn.commit()
        except Exception as log_error:
            # Log para ver el error si algo va mal
            print("⚠️ ERROR en /iclock/cdata:", log_error)
            print(f"Original body: {json.dumps(body)}")
        
        #No se debe retornar error para no bloquear al biométrico
        #raise HTTPException(status_code=500, detail=f"DB error: {e}")
        
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
    
    # ZKTeco espera texto plano "OK"
    return PlainTextResponse("OK")

def parse_adms_payload(raw_text):
    """
    Parsea el texto plano de ZKTeco (separado por tabuladores y saltos de línea).
    Retorna una lista de diccionarios con los datos encontrados.
    """
    records = []
    if not raw_text:
        return records

    lines = raw_text.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Diccionario para cada línea (cada marcaje)
        data = {}
        # El equipo separa los campos con tabuladores (\t)
        parts = line.split('\t')
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                data[key.strip()] = value.strip()
        
        if data:
            records.append(data)
            
    return records

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
            INSERT INTO logs (
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
