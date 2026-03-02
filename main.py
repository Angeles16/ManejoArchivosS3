import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

class S3Download:
    def __init__(self, bucket_name, path_s3, local_dir):
        self.bucket_name = bucket_name
        self.path_s3 = path_s3
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.session = boto3.Session(region_name='us-east-1')
        self.s3_client = self.session.client('s3')

    def get_files_s3(self):
        print(f"Buscando archivos en s3://{self.bucket_name}/{self.path_s3}")
        archivos_s3 = set()
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            operacion_paginator = paginator.paginate(Bucket=self.bucket_name, Prefix=self.path_s3)

            for page in operacion_paginator:
                if 'Contents' not in page:
                    print(f"⚠️ No hay archivos nuevos.")
                    return

                print(f"Carga KEYs archivos S3 ")
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    if s3_key.endswith('/'): continue
                    archivos_s3.add(s3_key)

            return archivos_s3

        except (NoCredentialsError, PartialCredentialsError) :
            print("❌ Error: No se encontraron credenciales de AWS.")
            self.error_log("Error de credenciales o autenticacion", "No se encontraron credenciales de AWS.")
        except Exception as e:
            print(f"❌ Ocurrió un error: {e}")
            self.error_log(e, "Error al obtener archivos de s3")

    def get_files_local(self):
        print(f"Escaneando directorio local: {self.local_dir}")
        archivos_local = set()
        try:
            for file in self.local_dir.iterdir():
                if file.is_file():
                    archivos_local.add(f"{self.path_s3}{file.name}")
            return archivos_local
        except Exception as e:
            print(f"Error al obtener archivos locales: {e}")
            self.error_log(e, "Error al obtener facturas local")

    def descargar_archivo(self, s3_key):
        try:
            file_name = Path(s3_key).name
            dest_path = self.local_dir / file_name
            self.s3_client.download_file(self.bucket_name, s3_key, str(dest_path))
        except ClientError as e:
            print(f"❌ No se pudo descargar {s3_key}: {e}")
            self.error_log(e, "Error descargando archivo")

    def error_log(self, error_e, mensaje_personalizado):
        BASE_DIR = Path(__file__).resolve().parent
        log_path = BASE_DIR / Path("log_errores.log")
        logging.basicConfig(
            filename=log_path,
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        detalle_log = f"{mensaje_personalizado} | Error Técnico: {str(error_e)}"
        logging.error(detalle_log)

    def ejectura_carga(self, prefix=""):
        print("Iniciondo proceso descarga de archivos S3...")
        cloud_keys = self.get_files_s3()
        local_keys = self.get_files_local()
        if not cloud_keys:
            print("⚠️ No hay archivos en S3 para procesar.")
            return

        faltantes = list(cloud_keys - local_keys)

        if not faltantes:
            print(f"✅ Todo está al día. No hay archivos nuevos que descargar.")
            return

        print(f"📝 Se encontraron {len(faltantes)} archivos nuevos.")

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.descargar_archivo, faltantes)

        print(f"✨ Proceso finalizado con éxito.")

if __name__ == "__main__":
    TIME_D = datetime.now()
    YEAR = TIME_D.year
    MONTH = TIME_D.month
    S3_BUCKET = "gd-archivospaginas"
    S3_PATH = f"LogFacturacion/FacturasFEL/{YEAR}/{MONTH}/"
    LOCAL_DIR = Path(f"C:/Users/Desarrollo/Documents/Tareas Infra/Tarea 31675/proyecto-s3/facturas/{YEAR}/{MONTH}/")
    sync = S3Download(S3_BUCKET, S3_PATH, LOCAL_DIR)
    sync.ejectura_carga()