# Imagen base
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar todos los archivos del proyecto
COPY . .

# Instalar dependencias necesarias
RUN pip install --no-cache-dir flask flask-cors polars openpyxl reportlab pillow python-dateutil

# Crear carpetas necesarias
RUN mkdir -p logs temp_uploads temp_downloads static

# Exponer puerto 8081
EXPOSE 8081

# Comando para ejecutar la app
CMD ["python", "app.py"]
