"""
Pre-Validador SIPOT (Polars + JSON extendido + Logs)
------------------------------------------------------------
✅ Polars >= 0.20.31
✅ Valida todas las columnas correctamente
✅ Agrupa rangos contiguos de celdas vacías o erróneas
✅ Respeta ceros ('0', 0, 0.0) como válidos
✅ Genera JSON extendido con:
   - id_formato (celda A1)
   - Titulo (celda A3)
   - Nombre Corto (celda D3)
   - data (usa fila 7 como encabezados visibles)
✅ Logs diarios estilo clásico
"""

# ===============================================================
# IMPORTS
# ===============================================================
import polars as pl
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import json, re, os, uuid, threading, tempfile, time, logging, math
from logging.handlers import TimedRotatingFileHandler
from openpyxl import load_workbook
from dateutil import parser

# ===============================================================
# CONFIG FLASK Y CARPETAS
# ===============================================================
app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = 'temp_uploads'
DOWNLOAD_FOLDER = 'temp_downloads'
LOG_FOLDER = 'logs'
for folder in (UPLOAD_FOLDER, DOWNLOAD_FOLDER, LOG_FOLDER):
    os.makedirs(folder, exist_ok=True)

# ===============================================================
# CONFIG LOG DIARIO
# ===============================================================
log_path = os.path.join(LOG_FOLDER, "validaciones.log")
handler = TimedRotatingFileHandler(log_path, when="midnight", interval=1,
                                   backupCount=30, encoding='utf-8')
handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger = logging.getLogger("validador")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

tasks = {}

# ===============================================================
# FUNCIONES AUXILIARES
# ===============================================================
def es_numero(v):
    try:
        float(v); return True
    except (ValueError, TypeError):
        return False

def es_fecha(v):
    try:
        parser.parse(str(v)); return True
    except Exception:
        return False

def es_hora(v):
    return bool(re.match(r'^([01]\d|2[0-3]):([0-5]\d)(:([0-5]\d))?$', str(v).strip()))

def es_url(v):
    return str(v).strip().lower().startswith(('http://', 'https://'))

def es_anio(v):
    return es_numero(v) and len(str(v).split('.')[0]) == 4

def esta_vacio(v):
    """Detecta vacíos reales pero respeta ceros ('0', 0, 0.0)."""
    if v is None:
        return True
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return True
        return False
    s = str(v).strip()
    if s in ('0', '0.0'):
        return False
    return s == '' or s.lower() in ('nan', 'none', 'null')

VALIDADORES = {
    '3': {'func': es_numero, 'nombre': 'Número'},
    '4': {'func': es_fecha,  'nombre': 'Fecha'},
    '5': {'func': es_hora,   'nombre': 'Hora (HH:MM)'},
    '6': {'func': es_numero, 'nombre': 'Moneda'},
    '7': {'func': es_url,    'nombre': 'URL'},
    '12': {'func': es_anio,  'nombre': 'Año (4 dígitos)'},
    '13': {'func': es_fecha, 'nombre': 'Fecha'},
}

def obtener_coordenada_excel(fila, col):
    """Convierte índices base-0 a coordenada Excel (A1)."""
    col_str = ""
    while col >= 0:
        col_str = chr(ord('A') + col % 26) + col_str
        col = col // 26 - 1
    return f"{col_str}{fila + 1}"

# ===============================================================
# CONVERSIÓN EXCEL → CSV
# ===============================================================
def convertir_excel_a_csv(ruta_excel):
    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    wb = load_workbook(ruta_excel, data_only=True)
    ws = wb.active
    with open(temp_csv.name, "w", encoding="utf-8") as f:
        for row in ws.iter_rows(values_only=True):
            vals = []
            for c in row:
                if c == 0 or c == 0.0 or c == "0":
                    vals.append("0")
                elif c is None:
                    vals.append("")
                else:
                    vals.append(str(c).replace(",", " ").replace("\n", " ").replace("\r", " "))
            f.write(",".join(vals) + "\n")
    return temp_csv.name

# ===============================================================
# PROCESAMIENTO PRINCIPAL
# ===============================================================
def procesar_archivo_en_segundo_plano(filepath, task_id):
    t0 = time.time()
    try:
        # --- Convertir Excel a CSV temporal ---
        ext = os.path.splitext(filepath)[1].lower()
        csv_path = convertir_excel_a_csv(filepath) if ext in (".xlsx", ".xls") else filepath

        # --- Leer CSV con Polars ---
        with open(csv_path, 'r', encoding='utf-8') as f:
            first = f.readline()
            n_cols = len(first.strip().split(',')) if first else 0
        schema = {str(i): pl.Utf8 for i in range(n_cols)}

        df = pl.read_csv(
            csv_path,
            has_header=False,
            infer_schema_length=0,
            null_values=['', 'NULL', 'null', 'NaN', 'nan'],
            schema_overrides=schema
        )

        # Quitar filas completamente vacías
        df = df.filter(pl.any_horizontal(~pl.col("*").is_null() & (pl.col("*").cast(str).str.strip_chars() != "")))

        # Fila 4 → reglas, fila 7 → encabezados visibles
        reglas  = df.row(3) if df.height > 3 else []
        headers_visibles = df.row(6) if df.height > 6 else []
        headers_visibles = [str(h or "").strip() for h in headers_visibles]
        datos = df.slice(7)

        lista_de_errores = []

        # ================= VALIDACIÓN =================
        for fila_idx, row in enumerate(datos.iter_rows()):
            abs_row_idx = fila_idx + 7
            for col_idx, valor in enumerate(row):
                if col_idx >= len(headers_visibles):
                    continue
                header = headers_visibles[col_idx]
                if header == '':
                    continue
                if esta_vacio(valor):
                    coord = obtener_coordenada_excel(abs_row_idx, col_idx)
                    lista_de_errores.append(f"Celda {coord} bajo '{header}' vacía.")
                regla = str(reglas[col_idx]).split('.')[0] if col_idx < len(reglas) else '0'
                if regla in VALIDADORES and not esta_vacio(valor):
                    val = VALIDADORES[regla]
                    if not val['func'](valor):
                        coord = obtener_coordenada_excel(abs_row_idx, col_idx)
                        lista_de_errores.append(f"Celda {coord} ('{valor}') inválida. Se esperaba: {val['nombre']}.")

        # ================= AGRUPAR ERRORES =================
        if lista_de_errores:
            patron = re.compile(r"Celda\s+([A-Z]+)(\d+)\s+(.*)")
            por_col_y_msg = {}
            for err in lista_de_errores:
                m = patron.search(err)
                if not m:
                    continue
                col = m.group(1)
                fila = int(m.group(2))
                msg = m.group(3).strip()
                por_col_y_msg.setdefault((col, msg), []).append(fila)

            bloques = []
            for (col, msg), filas in por_col_y_msg.items():
                filas.sort()
                ini = filas[0]
                prev = filas[0]
                for f in filas[1:]:
                    if f == prev + 1:
                        prev = f
                        continue
                    if ini == prev:
                        bloques.append(f"Celda {col}{ini} {msg}")
                    else:
                        bloques.append(f"Celda {col}{ini} hasta {col}{prev} {msg}")
                    ini = prev = f
                if ini == prev:
                    bloques.append(f"Celda {col}{ini} {msg}")
                else:
                    bloques.append(f"Celda {col}{ini} hasta {col}{prev} {msg}")

            lista_de_errores = bloques
            n_err = len(lista_de_errores)
            dur = round(time.time() - t0, 1)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            logger.info(f"[{task_id}] Archivo: {os.path.basename(filepath)} | Tamaño: {size_mb:.2f} MB | "
                        f"Errores: {n_err} | Tiempo: {dur:.1f}s | Estado: ERROR")
            tasks[task_id] = {'status': 'complete', 'result': {'status': 'error', 'errors': lista_de_errores}}
            return

        # ===================== JSON BACKEND ==========================
        formato = df[0, 0] if df.height > 0 else "Formato no encontrado"

        # Fila 7 como encabezados de datos
        headers_backend = df.row(6) if df.height > 6 else []
        headers_backend = [str(h or "header_sin_nombre").strip() for h in headers_backend]
        datos_backend = df.slice(7)

        # --- Extraer Titulo (A3) y Nombre Corto (D3) ---
        titulo = df[2, 0] if df.height > 2 and df.width > 0 else ""
        nombre_corto = df[2, 3] if df.height > 2 and df.width > 3 else ""

        # --- Generar registros ---
        registros = []
        for r in datos_backend.iter_rows():
            registro = {}
            for idx, valor in enumerate(r):
                if idx < len(headers_backend):
                    header = headers_backend[idx]
                    registro[header] = str(valor or "")
            registros.append(registro)

        # --- Estructura final ---
        out_json = {
            "id_formato": str(formato),
            "Titulo": str(titulo).strip(),
            "Nombre Corto": str(nombre_corto).strip(),
            "data": registros
        }

        json_path = os.path.join(DOWNLOAD_FOLDER, f"{task_id}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(out_json, f, ensure_ascii=False, indent=2)

        dur = round(time.time() - t0, 1)
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"[{task_id}] Archivo: {os.path.basename(filepath)} | Tamaño: {size_mb:.2f} MB | "
                    f"Errores: 0 | Tiempo: {dur:.1f}s | Estado: OK | Nombre Corto: {nombre_corto}")

        tasks[task_id] = {'status': 'complete',
                          'result': {'status': 'success', 'download_file': f"{task_id}.json"}}

    except Exception as e:
        dur = round(time.time() - t0, 1)
        logger.error(f"[{task_id}] Error inesperado: {str(e)} | Tiempo: {dur:.1f}s")
        tasks[task_id] = {'status': 'failed', 'error': str(e)}

    finally:
        if os.path.exists(filepath):
            os.remove(filepath)
        if 'csv_path' in locals() and csv_path != filepath and os.path.exists(csv_path):
            os.remove(csv_path)

# ===============================================================
# ENDPOINTS
# ===============================================================
@app.route('/')
def home():
    return send_from_directory('.', 'index.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'archivo' not in request.files:
        return jsonify({"error": "No se encontró archivo"}), 400
    archivo = request.files['archivo']
    if archivo.filename == '':
        return jsonify({"error": "No se seleccionó archivo"}), 400
    filename = f"{uuid.uuid4()}_{archivo.filename}"
    ruta = os.path.join(UPLOAD_FOLDER, filename)
    archivo.save(ruta)
    task_id = str(uuid.uuid4())
    tasks[task_id] = {'status': 'processing'}
    threading.Thread(target=procesar_archivo_en_segundo_plano, args=(ruta, task_id)).start()
    return jsonify({'task_id': task_id})

@app.route('/status/<task_id>')
def status(task_id):
    return jsonify(tasks.get(task_id, {'status': 'not_found'}))

@app.route('/download/<filename>')
def download(filename):
    return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)

# ===============================================================
# EJECUCIÓN
# ===============================================================
if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1')
# ===============================================================