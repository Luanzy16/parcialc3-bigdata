import boto3
import csv
from io import StringIO
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime
import os

# --- Configuración S3 ---
# ¡IMPORTANTE! Reemplaza "tu-nombre-de-bucket-s3-para-headlines" con el nombre REAL de tu bucket S3.
S3_BUCKET_NAME = "parcial3luis" # ¡Mantén tu nombre de bucket real aquí!
S3_RAW_PREFIX = "headlines/raw"
S3_FINAL_PREFIX = "headlines/final"

# El cliente S3 se inicializará directamente, esperando credenciales de AWS configuradas.
def get_s3_client():
    """
    Retorna un cliente S3 real.
    Asegúrate de que tus credenciales de AWS estén configuradas (ej. con 'aws configure').
    """
    return boto3.client('s3', region_name='sa-east-1') # Usa la región de tu bucket real

def upload_to_s3(file_content, object_name):
    """Sube el contenido del archivo a S3."""
    s3_client = get_s3_client()
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=object_name, Body=file_content, ContentType='text/html' if object_name.endswith('.html') else 'text/csv')
        print(f"Archivo subido a S3: s3://{S3_BUCKET_NAME}/{object_name}")
        return True
    except Exception as e:
        print(f"Error al subir a S3: {e}")
        return False

def download_from_s3(bucket, key):
    """Descarga el contenido de un archivo de S3."""
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error al descargar s3://{bucket}/{key}: {e}")
        return None

def extract_news_data(html_content, base_url, newspaper_name):
    """
    Extrae la categoría, titular y enlace de las noticias de un contenido HTML
    basado en el nombre del periódico.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    news_items = []
    processed_links = set() # Usar un set para evitar duplicados de manera eficiente

    if newspaper_name == "eltiempo":
        # (Lógica para El Tiempo se mantiene igual, omitida aquí por brevedad pero presente en el script completo)
        # Nuevo enfoque para El Tiempo: buscar elementos <article> con data-category
        articles = soup.find_all('article', attrs={'data-category': True})
        for article in articles:
            category_text = article.get('data-category', "Sin Categoría").strip()
            if category_text and category_text != "Sin Categoría":
                # Para El Tiempo, si data-category existe, la tomamos.
                # No hay un print específico por categoría aquí a menos que se solicite.
                pass # La categoría se usa directamente.
            category = category_text if category_text else "Sin Categoría"

            title_link_tag = article.find('a', class_='c-articulo__titulo__txt', href=True)
            if not title_link_tag:
                title_link_tag = article.find('a', class_='c-article-block__title-link', href=True)
            if not title_link_tag:
                title_link_tag = article.find('a', class_='c-main-section__card__title', href=True)

            if title_link_tag:
                title = title_link_tag.get_text(strip=True)
                link_href = title_link_tag.get('href')
                if title and link_href:
                    link = urljoin(base_url, link_href)
                    if link not in processed_links:
                        news_items.append({
                            'category': category,
                            'title': title,
                            'link': link
                        })
                        processed_links.add(link)
                        
        # Patrones adicionales para El Tiempo para mayor cobertura
        article_content_blocks = soup.find_all('div', class_='c-article-block__content')
        for block in article_content_blocks:
            category = "Sin Categoría"
            cat_span = block.find('span', class_='c-article-block__section')
            if cat_span:
                category_text = cat_span.get_text(strip=True)
                if category_text:
                    category = category_text

            link_tag = block.find('a', class_='c-article-block__title-link', href=True)
            if link_tag:
                title = link_tag.get_text(strip=True)
                link_href = link_tag.get('href')
                if title and link_href:
                    link = urljoin(base_url, link_href)
                    if link not in processed_links:
                        news_items.append({
                            'category': category,
                            'title': title,
                            'link': link
                        })
                        processed_links.add(link)
            
        main_section_cards = soup.find_all('div', class_='c-main-section__cards__item')
        for card_el_tiempo in main_section_cards: # Renombrada la variable card para evitar conflicto
            category = "Sin Categoría"
            cat_span = card_el_tiempo.find('span', class_='c-main-section__card__section')
            if cat_span:
                category_text = cat_span.get_text(strip=True)
                if category_text:
                    category = category_text

            link_tag = card_el_tiempo.find('a', class_='c-main-section__card__title', href=True)
            if link_tag:
                title = link_tag.get_text(strip=True)
                link_href = link_tag.get('href')
                if title and link_href:
                    link = urljoin(base_url, link_href)
                    if link not in processed_links:
                        news_items.append({
                            'category': category,
                            'title': title,
                            'link': link
                        })
                        processed_links.add(link)

    elif newspaper_name == "elespectador":
        # --- Lógica de Extracción para El Espectador (Mejorada con más selectores) ---
    
        card_selectors = [
            "div.CardLayout-Container",
            "div.Card",
            "div.Teaser-container",
            "article.Content",
            "div.card-body",
            # Nuevos selectores añadidos para cubrir más patrones de diseño
            "div[data-pf-type='BlockArticle']",  # Común en sitios que usan Piano Publisher Framework
            "div[data-pf-type='BlockPromo']",    # Otro tipo de bloque de contenido
            "div.promo-container",               # Contenedor genérico para promociones/noticias
            "div.article-container",             # Contenedor genérico para artículos
            "div.news-item",                     # Contenedor genérico para ítems de noticias
            "div.story-card",                    # Un patrón común para tarjetas de noticias
            "div.headline-card",                 # Otro patrón para tarjetas de titulares
            "section.main-content article",      # Artículos dentro de la sección principal de contenido
            "div.section-body .row .col-md-4",    # Patrón común en diseños de cuadrícula
            "div.listing-item",                  # Elementos en listados de noticias
            "div.news-card"                      # Otro nombre común para tarjetas de noticias
        ]
    
        for selector in card_selectors:
            article_cards = soup.select(selector)
    
            for card in article_cards:
                title = None
                link = None
                current_category_for_card = "Sin Categoría"
    
                # --- Extraer Título y Enlace ---
                # Prioridad 1: h2 con clases específicas (ej. Card-Title, Promo-title) conteniendo un <a>
                title_tag_h2 = card.find('h2', class_=lambda c: c and ('Card-Title' in c or 'Promo-title' in c))
                title_link_tag = None
                if title_tag_h2:
                    title_link_tag = title_tag_h2.find('a', href=True)
    
                # Prioridad 2: Cualquier h1, h2, h3, h4 que contenga un <a>
                if not title_link_tag:
                    heading_tag = card.find(['h1', 'h2', 'h3', 'h4'])
                    if heading_tag:
                        title_link_tag = heading_tag.find('a', href=True)
    
                # Prioridad 3: Para 'div.card-body' o contenedores similares, buscar enlaces principales
                if not title_link_tag and ('card-body' in selector or 'container' in selector or 'item' in selector):
                    # Intentar encontrar el enlace principal que no sea un pie de página o de navegación
                    candidate_links = card.find_all('a', href=True)
                    # Filtrar enlaces que parezcan ser de artículos (ej. tienen texto significativo)
                    for link_candidate in candidate_links:
                        link_text = link_candidate.get_text(strip=True)
                        # Excluir enlaces cortos, solo números o que son claramente de navegación
                        if link_text and len(link_text) > 10 and not link_text.isdigit() and not link_candidate.find_parent(class_=lambda c: c and ('footer' in c or 'nav' in c)):
                            # Si el enlace está dentro de un h-tag o es el único enlace principal
                            if link_candidate.find_parent(['h1', 'h2', 'h3', 'h4']) or \
                               (len(candidate_links) == 1 and link_candidate.get_text(strip=True)):
                                title_link_tag = link_candidate
                                break
                    # Si aún no se encontró, tomar el primer enlace con texto significativo
                    if not title_link_tag and candidate_links:
                        for link_candidate in candidate_links:
                            if link_candidate.get_text(strip=True) and not link_candidate.get_text(strip=True).isdigit():
                                title_link_tag = link_candidate
                                break
    
    
                if title_link_tag:
                    title_text_candidate = title_link_tag.get_text(strip=True)
                    # Si el texto del enlace está vacío pero su padre h-tag tiene texto, usar el del padre
                    if not title_text_candidate and title_link_tag.parent and title_link_tag.parent.name.startswith(('h1', 'h2', 'h3', 'h4')):
                        title_text_candidate = title_link_tag.parent.get_text(strip=True)
    
                    if title_text_candidate:
                        title = title_text_candidate
                        link_href = title_link_tag.get('href')
                        if link_href:
                            link = urljoin(base_url, link_href)
    
                # --- Extraer Categoría ---
                # Prioridad 1: Div contenedor de sección con h4 y enlace
                section_container_div = card.find('div', class_='Card-SectionContainer')
                if section_container_div:
                    category_h4_tag = section_container_div.find('h4', class_='Card-Section')
                    if category_h4_tag:
                        category_a_tag = category_h4_tag.find('a')
                        if category_a_tag:
                            category_text = category_a_tag.get_text(strip=True)
                            if category_text:
                                current_category_for_card = category_text
    
                # Prioridad 2: h4.Card-Section directo con enlace (si no se encontró por P1)
                if current_category_for_card == "Sin Categoría":
                    direct_category_h4 = card.find('h4', class_='Card-Section')
                    if direct_category_h4:
                        category_a_tag = direct_category_h4.find('a')
                        if category_a_tag:
                            category_text = category_a_tag.get_text(strip=True)
                            if category_text:
                                current_category_for_card = category_text
    
                # Prioridad 3: span.section-name (si no se encontró por P1 o P2)
                if current_category_for_card == "Sin Categoría":
                    category_span_tag = card.find('span', class_='section-name')
                    if category_span_tag:
                        category_text = category_span_tag.get_text(strip=True)
                        if category_text:
                            current_category_for_card = category_text
    
                # Prioridad 4: atributo data-category en la tarjeta (si no se encontró antes)
                if current_category_for_card == "Sin Categoría":
                    data_category_attr = card.get('data-category')
                    if data_category_attr:
                        category_text = data_category_attr.strip()
                        if category_text and category_text != "Sin Categoría":
                            current_category_for_card = category_text
    
                # Añadir a news_items si es válido y no duplicado
                if title and link and link not in processed_links:
                    news_item = {
                        'category': current_category_for_card,
                        'title': title,
                        'link': link
                    }
                    news_items.append(news_item)
                    processed_links.add(link)
                    # Imprime cada noticia extraída
                    print(f"Noticia extraída: Categoría: '{news_item['category']}', Título: '{news_item['title']}', Enlace: '{news_item['link']}'")
    
        # Al finalizar la extracción para el periódico, imprime el total
        print(f"Número total de noticias extraídas para El Espectador: {len(news_items)}")
        
    return news_items

def handler(event, context):
    """
    Función principal que se activa por un evento de S3 (cuando un archivo HTML llega a 'raw').
    """
    print(f"Evento S3 recibido: {event}")

    for record in event['Records']:
        bucket_name_event = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        print(f"Procesando archivo: s3://{bucket_name_event}/{object_key}")

        if not object_key.startswith(f"{S3_FINAL_PREFIX}/") and object_key.endswith('.html') and object_key.startswith(f"{S3_RAW_PREFIX}/"):
            try:
                html_content = download_from_s3(bucket_name_event, object_key) 
                if html_content is None:
                    print(f"No se pudo descargar {object_key}, saltando procesamiento.")
                    continue

                parts = object_key.split('/')
                filename = parts[-1]
                
                periodico_name_raw = filename.split('-')[0] 
                
                base_url = ""
                periodico = "" 
                
                if "eltiempo" in periodico_name_raw:
                    periodico = "eltiempo"
                    base_url = "https://www.eltiempo.com/"
                elif "elespectador" in periodico_name_raw:
                    periodico = "elespectador"
                    base_url = "https://www.elespectador.com/"
                    print(f"\nIniciando extracción para El Espectador: {filename}") # Added for clarity
                elif "publimetro" in periodico_name_raw: 
                    periodico = "publimetro"
                    base_url = "https://www.publimetro.co/"
                else:
                    print(f"Periódico desconocido en el nombre del archivo: {filename}. Saltando.")
                    continue

                news_data = extract_news_data(html_content, base_url, periodico) 
                print(f"Noticias extraídas para {periodico} ({len(news_data)}): {(news_data[:2] if news_data else 'Ninguna')}...") 
                
                if not news_data:
                    print(f"No se extrajeron noticias para {object_key}. No se generará CSV.")
                    continue

                csv_buffer = StringIO()
                fieldnames = ['category', 'title', 'link']
                writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
                writer.writeheader()
                for row in news_data:
                    writer.writerow(row)
                
                csv_content = csv_buffer.getvalue()

                try:
                    date_parts_match = filename.replace('.html', '').split('-')
                    if len(date_parts_match) >= 4: 
                        year = date_parts_match[-3]
                        month = date_parts_match[-2]
                        day = date_parts_match[-1]
                        if not (year.isdigit() and len(year) == 4 and \
                                month.isdigit() and 1 <= int(month) <= 12 and \
                                day.isdigit() and 1 <= int(day) <= 31):
                            raise ValueError("Formato de fecha inválido en el nombre del archivo.")
                    else:
                        raise ValueError("No se pudieron extraer suficientes partes de fecha del nombre del archivo.")

                except (IndexError, ValueError) as e_date:
                    print(f"Advertencia: No se pudo parsear la fecha del nombre del archivo '{filename}' ({e_date}). Usando fecha actual para el path S3.")
                    now = datetime.now()
                    year = now.strftime("%Y")
                    month = now.strftime("%m")
                    day = now.strftime("%d")

                s3_csv_object_key = (
                    f"{S3_FINAL_PREFIX}/periodico={periodico}/year={year}/month={month}/day={day}/"
                    f"{periodico}-headlines-{year}-{month}-{day}.csv"
                )

                upload_success = upload_to_s3(csv_content, s3_csv_object_key)
                if not upload_success:
                    print(f"Fallo al subir el CSV para {object_key}")

            except Exception as e:
                print(f"Error procesando {object_key}: {e}")
        else:
            print(f"Archivo {object_key} no elegible para procesamiento (no es HTML bajo {S3_RAW_PREFIX}/ o ya está en {S3_FINAL_PREFIX}/).")

    return {
        'statusCode': 200,
        'body': 'Procesamiento de archivos HTML completado.'
    }

if __name__ == '__main__':
    print(f"Ejecutando localmente la función Lambda de procesamiento, usando el bucket S3 real: {S3_BUCKET_NAME}...")
    
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    newspaper_to_test = "elespectador" 
    
    #nombre_archivo_prueba = f"{newspaper_to_test}-{current_date_str}.html"
    nombre_archivo_prueba = "elespectador-2025-05-30.html" # Using the specific file you provided
    
    s3_real_key_for_test = f"{S3_RAW_PREFIX}/{nombre_archivo_prueba}" 

    print(f"Intentando procesar el archivo de prueba: s3://{S3_BUCKET_NAME}/{s3_real_key_for_test}")
    print("Asegúrate de que este archivo exista en tu bucket S3 bajo esa ruta.")

    test_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "sa-east-1", 
                "eventTime": datetime.now().isoformat() + "Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigId",
                    "bucket": {
                        "name": S3_BUCKET_NAME, 
                        "arn": f"arn:aws:s3:::{S3_BUCKET_NAME}"
                    },
                    "object": {
                        "key": s3_real_key_for_test, 
                        "size": 1024, 
                        "eTag": "mock_etag_local_test",
                        "sequencer": "mock_sequencer_local_test"
                    }
                }
            }
        ]
    }
    
    handler(test_event, None)
        
    print("Ejecución local de la función de procesamiento terminada.")