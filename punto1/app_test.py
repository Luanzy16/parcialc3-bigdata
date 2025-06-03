import pytest
from unittest.mock import patch, MagicMock
import app

# Test 1: Verifica que se sube correctamente a S3
@patch("app.boto3.client")
def test_upload_to_s3_success(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.put_object.return_value = {}

    result = app.upload_to_s3("contenido de prueba", "ruta/archivo.html")
    
    mock_s3.put_object.assert_called_once()
    assert result == True

# Test 2: Verifica que se descarga correctamente una página
@patch("app.requests.get")
@patch("app.upload_to_s3")
def test_download_and_save_page_success(mock_upload, mock_requests_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html>Hola mundo</html>"
    mock_requests_get.return_value = mock_response
    mock_upload.return_value = True

    result = app.download_and_save_page("https://www.ejemplo.com", "sitio")

    mock_requests_get.assert_called_once_with("https://www.ejemplo.com")
    mock_upload.assert_called_once()
    assert result == True
