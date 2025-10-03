import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import gspread
from gspread_dataframe import set_with_dataframe
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from google.oauth2.service_account import Credentials
import os
import json

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
GOOGLE_SHEET_ID = "1tmIv9-f3U0yXNo87DsCwzv2kXUy7LVnmyS7XWfiEMvU"

# --- NUEVA ESTRUCTURA DE CATEGORÍAS ---
# Ahora cada categoría tiene su propia URL y su propia palabra clave para el filtro.
CATEGORIAS = {
    "Info_BENJAMÍ 1R":                { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22956097&codcompeticion=22536531&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_BENJAMÍ 2ON":               { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22949523&codcompeticion=22949518&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_ALEVÍ VERD SUB-11 PREF.":   { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22948452&codcompeticion=22948351&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_ALEVÍ VERMELL 1ª REGIONAL": { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22940155&codcompeticion=22940031&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_ALEVÍ BLANC PREFERENT":    { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22807752&codcompeticion=22536476&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_INFANTIL":                  { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22868791&codcompeticion=22868670&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_JUVENIL":                   { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22536427&codcompeticion=22536424&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_AMATEUR A":                 { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22536414&codcompeticion=22536413&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    "Info_AMATEUR B":                 { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22536417&codcompeticion=22536416&codtemporada=21&CodJornada=&CDetalle=1", "filtro": "BUNYOLA"},
    # --- NUEVA CATEGORÍA CON SU FILTRO PERSONALIZADO ---
    "Info_CADJUV_FEMENI":             { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=22876961&codcompeticion=22876781&codtemporada=21&CodJornada=1&CDetalle=1", "filtro": "RTVº MARRATXÍ DEL AT.M."}
}
# ==============================================================================

# --- PARTE 1: WEB SCRAPING ---
lista_total_partidos = []
print("Iniciando scraper con filtros por categoría...")
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

for nombre_pestana, info in CATEGORIAS.items():
    url = info["url"]
    palabra_clave_filtro = info["filtro"]
    
    print(f"\n--- Extrayendo datos para: {nombre_pestana} (Filtro: '{palabra_clave_filtro}') ---")
    try:
        driver.get(url)
        try:
            cookie_wait = WebDriverWait(driver, 5)
            accept_button = cookie_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.cmpboxbtnyes")))
            driver.execute_script("arguments[0].click();", accept_button)
            print("Pop-up de cookies aceptado.")
            time.sleep(1) 
        except TimeoutException: pass
        try:
            ad_wait = WebDriverWait(driver, 5)
            ad_close_button = ad_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "span.r89-sticky-top-close-button")))
            driver.execute_script("arguments[0].click();", ad_close_button)
            print("Banner de publicidad cerrado.")
        except TimeoutException: pass

        print("Cargando datos y esperando el contenido principal...")
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.card-body")))
        jornadas_blocks = driver.find_elements(By.CSS_SELECTOR, "div.card-body")
        partidos_encontrados_categoria = 0
        for jornada_block in jornadas_blocks:
            partidos = jornada_block.find_elements(By.CSS_SELECTOR, "div.row")
            for partido_div in partidos:
                try:
                    equipos = partido_div.find_elements(By.CSS_SELECTOR, "span.font_responsive")
                    if len(equipos) >= 2:
                        equipo_local = equipos[0].text.strip()
                        equipo_visitante = equipos[1].text.strip()
                        if palabra_clave_filtro.upper() in equipo_local.upper() or palabra_clave_filtro.upper() in equipo_visitante.upper():
                            info_div = partido_div.find_element(By.CSS_SELECTOR, "div.col-sm-5")
                            info_texto = info_div.text.strip().split('\n')
                            fecha_hora_texto = info_texto[-1]
                            fecha, hora = "", ""
                            if " - " in fecha_hora_texto:
                                partes = fecha_hora_texto.split(" - ")
                                fecha, hora = partes[0].strip(), partes[1].strip()
                            else:
                                fecha = fecha_hora_texto.strip()
                            partido = { "Categoria_Pestana": nombre_pestana, "Equipo Local": equipo_local, "Equipo Visitante": equipo_visitante, "Fecha": fecha, "Hora": hora }
                            lista_total_partidos.append(partido)
                            partidos_encontrados_categoria += 1
                except Exception: pass
        print(f"Se han encontrado {partidos_encontrados_categoria} partidos del club.")
    except Exception as e:
        print(f"ERROR al procesar '{nombre_pestana}': {e}")
driver.quit()

# --- PARTE 2: ACTUALIZAR GOOGLE SHEETS ---
if lista_total_partidos:
    try:
        print("\nConectando con Google Sheets...")
        creds_json_string = os.environ['GOOGLE_CREDENTIALS']
        creds_dict = json.loads(creds_json_string)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        print("Conexión exitosa. Actualizando pestañas...")
        df_total = pd.DataFrame(lista_total_partidos)
        for nombre_pestana, df_grupo in df_total.groupby('Categoria_Pestana'):
            print(f" - Actualizando pestaña: {nombre_pestana}")
            try:
                worksheet = sheet.worksheet(nombre_pestana)
                all_data = worksheet.get_all_values()
                extra_data = []
                found_header = False
                for row in all_data:
                    if any("DIRECCIÓN DEL CAMPO" in str(cell).upper() for cell in row):
                        found_header = True
                    if found_header:
                        extra_data.append(row)
                worksheet.clear()
                df_final = df_grupo[["Equipo Local", "Equipo Visitante", "Fecha", "Hora"]]
                set_with_dataframe(worksheet, df_final, include_index=False, include_column_header=True, resize=True)
                if extra_data:
                    print(f"   ... Preservando {len(extra_data)} filas de datos de campos.")
                    worksheet.append_row([])
                    worksheet.append_row([])
                    worksheet.append_rows(extra_data)
            except gspread.exceptions.WorksheetNotFound:
                print(f"   AVISO: No se encontró la pestaña '{nombre_pestana}'.")
        print("\n¡Proceso de actualización de Google Sheets completado!")
    except KeyError:
        print("\nERROR CRÍTICO: No se encontró el secreto 'GOOGLE_CREDENTIALS'.")
    except Exception as e:
        print(f"\nERROR CRÍTICO durante la conexión o actualización de Google Sheets: {e}")
else:
    print("\nNo se encontraron partidos para actualizar en Google Sheets.")
