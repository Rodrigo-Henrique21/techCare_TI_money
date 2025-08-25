import datetime
import logging
import azure.functions as func
from extracao import PipelineExtracao
from transform import PipelineTransformacao
import os

app = func.FunctionApp()

@app.schedule(schedule="0 0 7 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def run_pipeline(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed at %s', utc_timestamp)
    
    # Configurações
    TICKERS = ['PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3']
    data_inicio = '01/01/2019'
    data_fim = datetime.datetime.today().strftime('%d/%m/%Y')

    try:
        # Pipeline de Extração
        pipeline_extracao = PipelineExtracao(None, TICKERS, data_inicio, data_fim)
        pipeline_extracao.rodar()
        logging.info("Pipeline de extração concluído com sucesso!")

        # Pipeline de Transformação
        pipeline_transformacao = PipelineTransformacao(None, None)
        pipeline_transformacao.rodar()
        logging.info("Pipeline de transformação concluído com sucesso!")

        # Pipeline de Análise (camada gold)
        from analysis import IntegradorDados
        integrador = IntegradorDados()
        integrador.executar_analises()
        logging.info("Pipeline de análise concluído com sucesso!")

    except Exception as e:
        logging.error(f"Erro durante a execução do pipeline: {str(e)}")
        raise
