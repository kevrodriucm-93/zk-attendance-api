import logging
import os
import json
import traceback
from datetime import datetime

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import psycopg2
from psycopg2.extras import Json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de variables de entorno
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

# ============================================================
#  ENDPOINT OFICIAL ADMS: /iclock/cdata
# ============================================================
@app.api_route("/iclock/cdata", methods=["GET", "POST"])
async def iclock_cdata(request: Request):
    now = datetime.utcnow()
    params = dict(request.query_params)
    sn = params.get("SN") or params.get("sn") or "UNKNOWN_SN"
    table = params.get("table") or "UNKNOWN"
    
    conn = None
    cur = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        if request.method == "GET":
            # 1. Log del Handshake
            cur.execute(
                """
                INSERT INTO logs (zk_user_id, fecha_hora, dispositivo_codigo, sn, tipo, method, bruto_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                ("HANDSHAKE", now, sn, sn, "HANDSHAKE", "GET", Json({"query": params}))
            )
            conn.commit()

            # 2. Respuesta con parámetros para activar el envío de datos
            content = (
                f"GET OPTION FROM: SN={sn}\r\n"
                "RegistryCode=V6.60\r\n"
                "UpdateFlag=1\r\n"
                "PushProtVer=2.4.1\r\n"
                "ErrorDelay=30\r\n"
                "Delay=10\r\n"
                "TransInterval=1\r\n"
                "TransFlag=1111000000\r\n"
                "TimeZone=5\r\n"
                "Realtime=1\r\n"
            )
            return PlainTextResponse(content)

        else:
            # BLOQUE POST: Recibir marcaciones, info de dispositivo y logs de operación
            raw_bytes = await request.body()
            raw_text = raw_bytes.decode("utf-8", errors="ignore") if raw_bytes else ""
            lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]

            for line in lines:
                parts = line.split("\t")
                if not parts: continue
                
                first = parts[0]

                # --- Caso A: DEVINFO u OPLOG (Se mueven a la tabla LOGS) ---
                if first.startswith("~") or first.startswith("OPLOG"):
                    tipo_log = "DEVINFO" if first.startswith("~") else "OPLOG"
                    cur.execute(
                        """
                        INSERT INTO logs (
                            zk_user_id, fecha_hora, dispositivo_codigo, 
                            sn, tipo, method, bruto_json
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (tipo_log, now, sn, sn, tipo_log, "POST", Json({"raw": line}))
                    )
                
                # --- Caso B: ATTLOG (Marcaciones reales, se quedan en MARCAJES) ---
                elif len(parts) >= 2:
                    pin = parts[0].strip()
                    time_str = parts[1].strip()
                    verified = parts[2].strip() if len(parts) > 2 else None
                    status = parts[3].strip() if len(parts) > 3 else None
                    
                    try:
                        ts = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    except:
                        ts = now

                    cur.execute(
                        """
                        INSERT INTO marcajes (
                            zk_user_id, fecha_hora, dispositivo_codigo, 
                            sn, tipo, method, verified, status, bruto_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (zk_user_id, fecha_hora, dispositivo_codigo) DO NOTHING
                        """,
                        (pin, ts, sn, sn, "ATTLOG", "POST", verified, status, Json({"raw": line}))
                    )
            
            conn.commit()
    except Exception as e:
        # Log de errores en tabla secundaria para no perder el rastro
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO marcajes_error_log (error_message, dispositivo_codigo, original_body, stack_trace) VALUES (%s, %s, %s, %s)",
                    (str(e), sn, raw_text if 'raw_text' in locals() else str(params), traceback.format_exc())
                )
                conn.commit()
            except:
                pass
        print(f"❌ Error procesando datos de {sn}: {e}")

    finally:
        if cur: cur.close()
        if conn: conn.close()

    # Siempre responder OK para que el biométrico no se bloquee
    return PlainTextResponse("OK")

# ============================================================
#  ENDPOINTS DE COMANDOS (Obligatorios para el flujo ADMS)
# ============================================================
@app.get("/iclock/getrequest")
async def iclock_getrequest(request: Request):
    return PlainTextResponse("OK")

@app.post("/iclock/devicecmd")
async def iclock_devicecmd(request: Request):
    return PlainTextResponse("OK")