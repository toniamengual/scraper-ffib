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
import requests

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
GOOGLE_SHEET_ID = "1AL4xDpRp4PmyJvEohY7kCpjs6p61bdHzgmUZKmuTKHo"

# Token del bot de Telegram (guardado como secreto en GitHub Actions)
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')

# --- ESTRUCTURA DE CATEGORÍAS ---
# Cada categoría tiene su URL, filtro y el ID del canal de Telegram.
# URL vacía ("") = calendario aún no publicado por la FFIB. Se omite sin error.
# ⚠️  MAPEO TELEGRAM → pestaña Google Sheets:
#     BENJAMI BLANC  → Info_BENJAMÍ 1R   (confirmar si es correcto)
#     BENJAMI VERMELL→ Info_BENJAMÍ 2ON  (confirmar si es correcto)
#     INFANTIL F11   → Info_INFANTIL     (confirmar si es correcto)
#     CADET          → Info_CADJUV_FEMENI
CATEGORIAS = {
    # ── Categorías SIN calendario publicado aún ──────────────────────────────
    "Info_ESCOLETA":                  { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1004469746773" },
    "Info_PREBENJAMI":                { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1004347891667" },
    "Info_INFANTIL F7":               { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1003979746641" },

    # ── Categorías CON calendario temporada pasada (URLs pendientes de actualizar) ──
    "Info_BENJAMÍ 1R":                { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1004375714654" },  # BENJAMI BLANC
    "Info_BENJAMÍ 2ON":               { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1004399088046" },  # BENJAMI VERMELL
    "Info_ALEVÍ VERD SUB-11 PREF.":   { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1003815706145" },  # ALEVI VERD
    "Info_ALEVÍ VERMELL 1ª REGIONAL": { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1003940518226" },  # ALEVI VERMELL
    "Info_ALEVÍ BLANC PREFERENT":     { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1004349965204" },  # ALEVI BLANC
    "Info_INFANTIL":                  { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1003734265279" },  # INFANTIL F11
    "Info_JUVENIL":                   { "url": "", "filtro": "BUNYOLA",                  "telegram_channel": "-1004324337267" },  # JUVENIL
    "Info_CADJUV_FEMENI":             { "url": "", "filtro": "RTVº MARRATXÍ DEL AT.M.", "telegram_channel": "-1004465478173" },  # CADET

    # ── Categorías CON calendario temporada 22 ───────────────────────────────
    "Info_AMATEUR A":                 { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=23348366&codcompeticion=23348365&codtemporada=22&CodJornada=&CDetalle=1", "filtro": "BUNYOLA", "telegram_channel": "-1003877588580" },
    "Info_AMATEUR B":                 { "url": "https://www.ffib.es/Fed/NPcd/NFG_VisCalendario_Vis?cod_primaria=1000110&codgrupo=23433050&codcompeticion=23348367&codtemporada=22&CodJornada=&CDetalle=1", "filtro": "BUNYOLA", "telegram_channel": "-1004483561029" }
}
# ==============================================================================


# ==============================================================================
# FUNCIONES DE TELEGRAM Y DETECCIÓN DE CAMBIOS
# ==============================================================================

def enviar_telegram(channel_id, mensaje):
    """Envía un mensaje de texto a un canal de Telegram via Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        print("   [Telegram] TELEGRAM_BOT_TOKEN no configurado, saltando notificación.")
        return
    if not channel_id:
        print("   [Telegram] Canal no configurado para esta categoría, saltando.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={
            'chat_id': channel_id,
            'text': mensaje,
            'parse_mode': 'HTML'
        }, timeout=10)
        if resp.status_code == 200:
            print(f"   [Telegram] ✅ Mensaje enviado a {channel_id}")
        else:
            print(f"   [Telegram] ❌ Error {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"   [Telegram] ❌ Error de conexión: {e}")


def leer_partidos_actuales(worksheet):
    """
    Lee los partidos actuales de una pestaña de Sheets antes de sobreescribir.
    Devuelve un dict con clave 'Local|Visitante' y valor {fecha, hora}.
    """
    try:
        records = worksheet.get_all_records()
        partidos = {}
        for row in records:
            local = str(row.get('Equipo Local', '')).strip()
            visitante = str(row.get('Equipo Visitante', '')).strip()
            if local and visitante:
                clave = f"{local}|{visitante}"
                partidos[clave] = {
                    'local': local,
                    'visitante': visitante,
                    'fecha': str(row.get('Fecha', '')).strip(),
                    'hora': str(row.get('Hora', '')).strip()
                }
        return partidos
    except Exception as e:
        print(f"   [Cambios] No se pudieron leer datos anteriores: {e}")
        return {}


def detectar_cambios_y_notificar(nombre_pestana, partidos_antes, partidos_despues, channel_id):
    """
    Compara los partidos antes y después de la actualización.
    Genera y envía mensajes de Telegram para cada cambio detectado.
    """
    nombre_cat = nombre_pestana.replace("Info_", "").upper()
    cambios_total = 0

    # 1) Partidos nuevos (aparecen en los nuevos, no estaban antes)
    for clave, p in partidos_despues.items():
        if clave not in partidos_antes:
            hora_str = f" · {p['hora']}" if p['hora'] else ""
            mensaje = (
                f"🆕 <b>NOU PARTIT — {nombre_cat}</b>\n\n"
                f"⚽ {p['local']} vs {p['visitante']}\n"
                f"📅 {p['fecha']}{hora_str}"
            )
            print(f"   [Cambios] NUEVO partido: {p['local']} vs {p['visitante']} ({p['fecha']})")
            enviar_telegram(channel_id, mensaje)
            cambios_total += 1

    # 2) Partidos eliminados (estaban antes, ya no aparecen)
    for clave, p in partidos_antes.items():
        if clave not in partidos_despues:
            hora_str = f" · {p['hora']}" if p['hora'] else ""
            mensaje = (
                f"❌ <b>PARTIT CANCEL·LAT — {nombre_cat}</b>\n\n"
                f"⚽ {p['local']} vs {p['visitante']}\n"
                f"📅 {p['fecha']}{hora_str}"
            )
            print(f"   [Cambios] CANCELADO: {p['local']} vs {p['visitante']} ({p['fecha']})")
            enviar_telegram(channel_id, mensaje)
            cambios_total += 1

    # 3) Cambios de fecha u hora en partidos existentes
    for clave, p_nuevo in partidos_despues.items():
        if clave in partidos_antes:
            p_ant = partidos_antes[clave]
            fecha_cambio = p_ant['fecha'] != p_nuevo['fecha']
            hora_cambio = p_ant['hora'] != p_nuevo['hora']
            if fecha_cambio or hora_cambio:
                antes_str = p_ant['fecha'] + (f" · {p_ant['hora']}" if p_ant['hora'] else "")
                despues_str = p_nuevo['fecha'] + (f" · {p_nuevo['hora']}" if p_nuevo['hora'] else "")
                mensaje = (
                    f"⚠️ <b>CANVI D'HORARI — {nombre_cat}</b>\n\n"
                    f"⚽ {p_nuevo['local']} vs {p_nuevo['visitante']}\n"
                    f"📅 Abans:  {antes_str}\n"
                    f"📅 Ara:    {despues_str}"
                )
                print(f"   [Cambios] CAMBIO HORARIO: {p_nuevo['local']} vs {p_nuevo['visitante']}: {antes_str} → {despues_str}")
                enviar_telegram(channel_id, mensaje)
                cambios_total += 1

    if cambios_total == 0:
        print(f"   [Cambios] Sin cambios detectados en {nombre_pestana}.")
    else:
        print(f"   [Cambios] {cambios_total} cambio(s) notificado(s) en {nombre_pestana}.")

    return cambios_total


# ==============================================================================
# PARTE 1: WEB SCRAPING
# ==============================================================================
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

    if not url:
        print(f"\n--- Saltando '{nombre_pestana}': URL no disponible aún. ---")
        continue

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


# ==============================================================================
# PARTE 2: COMPARAR CON SHEETS Y NOTIFICAR CAMBIOS POR TELEGRAM
# ==============================================================================
if lista_total_partidos:
    try:
        print("\nConectando con Google Sheets...")
        creds_json_string = os.environ['GOOGLE_CREDENTIALS']
        creds_dict = json.loads(creds_json_string)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        print("Conexión exitosa. Procesando categorías...")

        df_total = pd.DataFrame(lista_total_partidos)

        for nombre_pestana, df_grupo in df_total.groupby('Categoria_Pestana'):
            print(f"\n - Procesando pestaña: {nombre_pestana}")
            channel_id = CATEGORIAS.get(nombre_pestana, {}).get('telegram_channel', '')

            try:
                worksheet = sheet.worksheet(nombre_pestana)

                # --- NUEVO: Leer datos actuales ANTES de sobreescribir ---
                print("   Leyendo datos actuales para comparación...")
                partidos_antes = leer_partidos_actuales(worksheet)

                # --- Preservar datos extra (tabla de campos) ---
                all_data = worksheet.get_all_values()
                extra_data = []
                found_header = False
                for row in all_data:
                    if any("DIRECCIÓN DEL CAMPO" in str(cell).upper() for cell in row):
                        found_header = True
                    if found_header:
                        extra_data.append(row)

                # --- Actualizar Sheets ---
                worksheet.clear()
                df_final = df_grupo[["Equipo Local", "Equipo Visitante", "Fecha", "Hora"]]
                set_with_dataframe(worksheet, df_final, include_index=False, include_column_header=True, resize=True)
                if extra_data:
                    print(f"   Preservando {len(extra_data)} filas de datos de campos.")
                    worksheet.append_row([])
                    worksheet.append_row([])
                    worksheet.append_rows(extra_data)

                # --- NUEVO: Construir dict de partidos nuevos y comparar ---
                partidos_despues = {}
                for _, row in df_final.iterrows():
                    local = str(row['Equipo Local']).strip()
                    visitante = str(row['Equipo Visitante']).strip()
                    if local and visitante:
                        clave = f"{local}|{visitante}"
                        partidos_despues[clave] = {
                            'local': local,
                            'visitante': visitante,
                            'fecha': str(row['Fecha']).strip(),
                            'hora': str(row['Hora']).strip()
                        }

                detectar_cambios_y_notificar(nombre_pestana, partidos_antes, partidos_despues, channel_id)

            except gspread.exceptions.WorksheetNotFound:
                print(f"   AVISO: No se encontró la pestaña '{nombre_pestana}'.")

        print("\n¡Proceso completado!")

    except KeyError:
        print("\nERROR CRÍTICO: No se encontró el secreto 'GOOGLE_CREDENTIALS'.")
    except Exception as e:
        print(f"\nERROR CRÍTICO durante la conexión o actualización de Google Sheets: {e}")
else:
    print("\nNo se encontraron partidos para actualizar en Google Sheets.")
