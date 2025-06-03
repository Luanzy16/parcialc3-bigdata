import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from punto3.app import handler 

CRAWLER_NAME = 'parcial3'

@patch('punto3.app.boto3.client')
def test_handler_success(mock_boto_client, capsys):
    mock_glue = MagicMock()
    mock_boto_client.return_value = mock_glue
    
    handler(CRAWLER_NAME)
    
    mock_glue.start_crawler.assert_called_once_with(Name=CRAWLER_NAME)
    
    captured = capsys.readouterr()
    assert f"Crawler '{CRAWLER_NAME}' iniciado exitosamente." in captured.out

@patch('punto3.app.boto3.client')
def test_handler_crawler_running_exception(mock_boto_client, capsys):
    mock_glue = MagicMock()
    error_response = {'Error': {'Code': 'CrawlerRunningException'}}
    mock_glue.start_crawler.side_effect = ClientError(error_response, 'StartCrawler')
    mock_boto_client.return_value = mock_glue
    handler(CRAWLER_NAME)
    
    captured = capsys.readouterr()
    assert f"El crawler '{CRAWLER_NAME}' ya está en ejecución." in captured.out

@patch('punto3.app.boto3.client')
def test_handler_other_client_error(mock_boto_client, capsys):
    mock_glue = MagicMock()
    error_response = {'Error': {'Code': 'SomeOtherError'}}
    mock_glue.start_crawler.side_effect = ClientError(error_response, 'StartCrawler')
    mock_boto_client.return_value = mock_glue
    
    handler(CRAWLER_NAME)
    
    captured = capsys.readouterr()
    assert "Error al ejecutar el crawler:" in captured.out

@patch('punto3.app.boto3.client')
def test_handler_unexpected_exception(mock_boto_client, capsys):
    mock_glue = MagicMock()
    mock_glue.start_crawler.side_effect = Exception("Error inesperado")
    mock_boto_client.return_value = mock_glue
    
    handler(CRAWLER_NAME)
    
    captured = capsys.readouterr()
    assert "Error inesperado:" in captured.out
