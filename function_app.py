
import datetime
import logging
import azure.functions as func
from extracao import PipelineExtracao
from transform import PipelineTransformacao
import os
from azure.monitor.opentelemetry import configure_azure_monitor
from opencensus.ext.azure.log_exporter import AzureLogHandler

# Configuração avançada de logger
logger = logging.getLogger("techcare.pipeline")
logger.setLevel(logging.INFO)

# Configurar Application Insights
connection_string = os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING')
if connection_string:
    logger.info("Configurando Application Insights com connection string")
    configure_azure_monitor()
    handler = AzureLogHandler(connection_string=connection_string)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
else:
    logger.warning("APPLICATIONINSIGHTS_CONNECTION_STRING não encontrada nas variáveis de ambiente")


app = func.FunctionApp()

@app.schedule(schedule="0 12 2 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def run_pipeline(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logger.info(f"Pipeline iniciado em {utc_timestamp}")

    if myTimer.past_due:
        logger.warning('Execução atrasada! O timer está past due.')

    logger.info('Iniciando execução do pipeline completo')
    
    # Configurações
    TICKERS = ['PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3']
    data_inicio = '01/01/2019'
    data_fim = datetime.datetime.today().strftime('%d/%m/%Y')
    logger.info(f"Configurações: Tickers={TICKERS}, Período={data_inicio} até {data_fim}")

    try:
        # Pipeline de Extração
        logger.info("Iniciando pipeline de extração...")
        pipeline_extracao = PipelineExtracao(None, TICKERS, data_inicio, data_fim)
        pipeline_extracao.rodar()
        logger.info("Pipeline de extração concluído com sucesso!")

        # Pipeline de Transformação
        logger.info("Iniciando pipeline de transformação...")
        pipeline_transformacao = PipelineTransformacao(None, None)
        pipeline_transformacao.rodar()
        logger.info("Pipeline de transformação concluído com sucesso!")

        # Pipeline de Análise (camada gold)
        from analysis import IntegradorDados
        logger.info("Iniciando pipeline de análise...")
        integrador = IntegradorDados()
        integrador.executar_analises()
        logger.info("Pipeline de análise concluído com sucesso!")
        
        logger.info("Pipeline completo executado com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline: {str(e)}", exc_info=True)
        logger.exception("Stack trace completo do erro:")
        raise
