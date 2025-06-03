import pytest
from unittest.mock import patch, MagicMock
from punto2.app import (
    extract_news_data,
    upload_to_s3,
    download_from_s3,
    get_s3_client
)

# Muestra de HTML para test de El Tiempo
SAMPLE_HTML_EL_TIEMPO = """
<html>
    <body>
        <article data-category="Política">
            <a class="c-articulo__titulo__txt" href="/noticia1.html">Noticia de prueba</a>
        </article>
    </body>
</html>
"""

SAMPLE_HTML_EL_ESPECTADOR = """
<html>
    <body>
        <div class="CardLayout-Container">
            <h2 class="Card-Title"><a href="/articulo-espectador.html">Título Espectador</a></h2>
        </div>
    </body>
</html>
"""

def test_extract_news_data_eltiempo():
    base_url = "https://www.eltiempo.com"
    result = extract_news_data(SAMPLE_HTML_EL_TIEMPO, base_url, "eltiempo")
    assert len(result) == 1
    assert result[0]['category'] == "Política"
    assert result[0]['title'] == "Noticia de prueba"
    assert result[0]['link'] == "https://www.eltiempo.com/noticia1.html"

def test_extract_news_data_elespectador():
    base_url = "https://www.elespectador.com"
    result = extract_news_data(SAMPLE_HTML_EL_ESPECTADOR, base_url, "elespectador")
    assert len(result) == 1
    assert result[0]['title'] == "Título Espectador"
    assert result[0]['link'] == "https://www.elespectador.com/articulo-espectador.html"

@patch('punto2.app.get_s3_client')
def test_upload_to_s3(mock_get_s3_client):
    # Simula el cliente y su método put_object
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.put_object.return_value = {}
    
    result = upload_to_s3("contenido de prueba", "headlines/raw/test.html")
    assert result is True
    mock_s3.put_object.assert_called_once()

@patch('punto2.app.get_s3_client')
def test_download_from_s3(mock_get_s3_client):
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_response = {'Body': MagicMock(read=lambda: b"contenido de prueba")}
    mock_s3.get_object.return_value = mock_response
    
    result = download_from_s3("parcial3luis", "headlines/raw/test.html")
    assert result == "contenido de prueba"
    mock_s3.get_object.assert_called_once()
