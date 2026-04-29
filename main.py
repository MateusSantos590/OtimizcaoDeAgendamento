"""
Projeto: Otimização de Agendamentos e Retenção (Enterprise Grade)
Descrição: Pipeline ETL com análise de dados para otimização de horários.
Padrões aplicados: Orientação a Objetos (OOP), Logging, Type Hinting, Tratamento de Erros.
"""
# Vamos falar sobre essas bibliotecas:

import sqlite3 # Biblioteca padrão do python que permite trabalhar com bancos de dados relacionais.
import pandas as pd # Biblioteca padrão do python que fornece estruturas de dados e ferramentas de análise.
from datetime import datetime, timedelta # Biblioteca padrão do python que fornece classes para manipulação de datas e horas.
import random # Biblioteca padrão do python que fornece funções para geração de números aleatórios.
import matplotlib.pyplot as plt # Biblioteca padrão do python que fornece funções para geração de gráficos.
import os # Biblioteca padrão do python que fornece funções para manipulação de arquivos e diretórios.
import logging # Biblioteca padrão do python que fornece funções para geração de logs.
from typing import List, Dict, Any, Optional # Biblioteca padrão do python que fornece funções para geração de logs.

# ==============================================================================
# 1. Configurações Globais do Projeto (Configuration Management)
# ==============================================================================
class Config:
    """Classe responsável por armazenar parâmetros e caminhos do sistema."""
    NUM_REGISTROS: int = 1500
    DIRETORIO_ATUAL: str = os.path.dirname(os.path.abspath(__file__))
    
    DB_PATH: str = os.path.join(DIRETORIO_ATUAL, 'agendamentos_barbearia.db')
    LOG_PATH: str = os.path.join(DIRETORIO_ATUAL, 'pipeline_etl.log')
    GRAFICO_PATH: str = os.path.join(DIRETORIO_ATUAL, 'grafico_ociosidade.png')
    
    STATUS_OPCOES: List[str] = ['Concluído', 'Concluído', 'Concluído', 'Ausente', 'Cancelado']
    SERVICOS_OPCOES: List[str] = ['Corte', 'Barba', 'Corte + Barba', 'Coloração', 'Sobrancelha']

# ==============================================================================
# 2. Configuração do Sistema de Logging (Rastreabilidade)
# ==============================================================================
def setup_logger() -> logging.Logger:
    """Configura o logger para gerar saída tanto no console quanto em arquivo."""
    logger = logging.getLogger('PipelineETL')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
        
        # Handler para salvar no arquivo
        file_handler = logging.FileHandler(Config.LOG_PATH, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # Handler para exibir no terminal
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
    return logger

logger = setup_logger()

# ==============================================================================
# 3. Classes do Pipeline (OOP & Clean Code)
# ==============================================================================

class ExtratorDados:
    """Módulo responsável pela ingestão/extração de dados brutos."""
    
    def extrair_api_mock(self, num_registros: int) -> List[Dict[str, Any]]:
        """Simula a extração de dados de uma API de agenda com sujeira injetada."""
        logger.info(f"Iniciando extração simulada de {num_registros} registros.")
        dados = []
        data_atual = datetime.now()
        
        try:
            for _ in range(num_registros):
                dias_atras = random.randint(0, 30)
                hora = random.randint(9, 18)
                minuto = random.choice([0, 30])
                
                data_agendamento = data_atual - timedelta(days=dias_atras)
                data_agendamento = data_agendamento.replace(hour=hora, minute=minuto, second=0, microsecond=0)
                
                cliente_id = random.randint(1000, 1500) if random.random() > 0.05 else None
                
                dados.append({
                    'id_agendamento': f"AGD-{random.randint(10000, 99999)}",
                    'id_cliente': cliente_id,
                    'servico': random.choice(Config.SERVICOS_OPCOES) if random.random() > 0.02 else None,
                    'data_hora': data_agendamento.strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.01 else 'Data_Invalida',
                    'status': random.choice(Config.STATUS_OPCOES)
                })
            
            logger.info("Extração concluída com sucesso.")
            return dados
            
        except Exception as e:
            logger.error(f"Erro crítico durante a extração de dados: {e}")
            raise

class TransformadorDados:
    """Módulo responsável pela higienização (Data Cleaning)."""
    
    def higienizar(self, dados_brutos: List[Dict[str, Any]]) -> pd.DataFrame:
        logger.info("Iniciando processo de Transformação/Higienização.")
        
        try:
            df = pd.DataFrame(dados_brutos)
            qtd_inicial = len(df)
            
            # 1. Filtro de Datas Inválidas
            df_limpo = df[df['data_hora'] != 'Data_Invalida'].copy()
            qtd_invalidas = qtd_inicial - len(df_limpo)
            if qtd_invalidas > 0:
                logger.warning(f"Descartados {qtd_invalidas} registros com 'data_hora' inválida.")
            
            # 2. Conversão de Tipos e Tratamento de Formatos Inválidos
            df_limpo['data_hora'] = pd.to_datetime(df_limpo['data_hora'], errors='coerce')
            df_limpo = df_limpo.dropna(subset=['data_hora'])
            
            # 3. Tratamento de Nulos
            df_limpo['servico'] = df_limpo['servico'].fillna('Serviço Não Informado')
            
            qtd_antes_nan = len(df_limpo)
            df_limpo = df_limpo.dropna(subset=['id_cliente'])
            qtd_orfaos = qtd_antes_nan - len(df_limpo)
            if qtd_orfaos > 0:
                logger.warning(f"Descartados {qtd_orfaos} registros sem 'id_cliente' (clientes órfãos).")
            
            df_limpo['id_cliente'] = df_limpo['id_cliente'].astype(int)
            
            # 4. Deduplicação (mantendo o registro mais recente no lote)
            qtd_antes_dup = len(df_limpo)
            df_limpo = df_limpo.drop_duplicates(subset=['id_agendamento'], keep='last')
            qtd_dup = qtd_antes_dup - len(df_limpo)
            if qtd_dup > 0:
                logger.warning(f"Descartados {qtd_dup} registros duplicados baseados no 'id_agendamento'.")
            
            logger.info(f"Transformação concluída. Registros válidos finais: {len(df_limpo)}")
            return df_limpo
            
        except Exception as e:
            logger.error(f"Erro durante a transformação dos dados com Pandas: {e}")
            raise

class CarregadorDados:
    """Módulo responsável por persistir os dados no destino (Data Warehouse/Database)."""
    
    def salvar_banco(self, df: pd.DataFrame, caminho_banco: str) -> None:
        logger.info(f"Iniciando carga (Load) no banco de dados SQLite: {os.path.basename(caminho_banco)}")
        
        try:
            # Context manager (with) garante que a conexão será fechada corretamente
            with sqlite3.connect(caminho_banco) as conn:
                df.to_sql('historico_agendamentos', conn, if_exists='replace', index=False, chunksize=1000)
                
            logger.info("Carga de dados no SQLite concluída com sucesso.")
            
        except sqlite3.Error as e:
            logger.error(f"Erro de banco de dados ao salvar a tabela: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado durante a carga de dados: {e}")
            raise

class AnalisadorNegocios:
    """Módulo de Business Intelligence (BI) e Geração de Insights."""
    
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def gerar_relatorio_ociosidade(self) -> None:
        logger.info("Iniciando motor de Análise de Negócios (BI).")
        
        try:
            with sqlite3.connect(self.caminho_banco) as conn:
                df = pd.read_sql_query("SELECT * FROM historico_agendamentos", conn)
                
            if df.empty:
                logger.warning("O banco de dados está vazio. Abortando análise.")
                return
                
            df['data_hora'] = pd.to_datetime(df['data_hora'])
            df['hora_agendamento'] = df['data_hora'].dt.hour
            df['foi_perdido'] = df['status'].isin(['Ausente', 'Cancelado'])
            
            ociosidade_por_hora = df.groupby('hora_agendamento')['foi_perdido'].mean() * 100
            
            if ociosidade_por_hora.empty:
                logger.warning("Não há dados de horário válidos para gerar insights de evasão.")
                return
                
            hora_critica = int(ociosidade_por_hora.idxmax())
            taxa_critica = float(ociosidade_por_hora.max())
            
            # Log de Insights Acionáveis
            logger.info(f"Insight de BI gerado. Horário crítico: {hora_critica}h com {taxa_critica:.2f}% de perdas.")
            
            print("\n" + "="*60)
            print(">> INSIGHT ACIONÁVEL DE NEGÓCIOS (BI)")
            print("="*60)
            print(f"ALERTA: O horário comercial com MAIOR EVASÃO de clientes é às {hora_critica}h.")
            print(f"Isso significa que {taxa_critica:.2f}% dos agendamentos marcados para esse horário são cancelados ou sofrem no-show.")
            print("\nSUGESTÃO DO SISTEMA:")
            print(f"- Configurar automação via webhook para disparo de WhatsApp 2 horas antes (às {hora_critica - 2}h).")
            print(f"- Liberar promoções de 'Happy Hour' limitados para este período para forçar engajamento.")
            print("="*60 + "\n")
            
            self._gerar_grafico(ociosidade_por_hora, hora_critica)
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatórios de BI: {e}")

    def _gerar_grafico(self, serie_dados: pd.Series, hora_destaque: int) -> None:
        """Método encapsulado interno para cuidar apenas do design do gráfico."""
        try:
            plt.figure(figsize=(10, 5))
            cores = ['#e74c3c' if h == hora_destaque else '#3498db' for h in serie_dados.index]
            
            serie_dados.plot(kind='bar', color=cores)
            plt.title('Taxa de Evasão (Cancelamentos/Ausências) por Horário (%)', fontsize=14, fontweight='bold')
            plt.xlabel('Horário Agendado (h)', fontsize=12)
            plt.ylabel('Taxa de Perda (%)', fontsize=12)
            plt.grid(axis='y', linestyle='--', alpha=0.4)
            plt.xticks(rotation=0)
            
            plt.tight_layout()
            plt.savefig(Config.GRAFICO_PATH)
            plt.close() # Evita memory leak ao liberar a figura da memória
            logger.info(f"Dashboard gráfico renderizado e salvo em: {Config.GRAFICO_PATH}")
            
        except ImportError:
            logger.warning("Matplotlib não instalado. O Dashboard gráfico não foi gerado.")
        except Exception as e:
            logger.error(f"Erro ao renderizar o gráfico: {e}")

class OrquestradorETL:
    """Classe Principal: Orquestra todo o ciclo de vida do Pipeline."""
    
    def executar(self) -> None:
        logger.info("="*50)
        logger.info("INICIANDO PIPELINE DE DADOS (ENTERPRISE GRADE)")
        logger.info("="*50)
        
        try:
            extrator = ExtratorDados()
            transformador = TransformadorDados()
            carregador = CarregadorDados()
            analisador = AnalisadorNegocios(Config.DB_PATH)
            
            # Execução estruturada do ETL
            dados_brutos = extrator.extrair_api_mock(Config.NUM_REGISTROS)
            dados_limpos = transformador.higienizar(dados_brutos)
            carregador.salvar_banco(dados_limpos, Config.DB_PATH)
            
            # Análises e Geração de Valor
            analisador.gerar_relatorio_ociosidade()
            
            logger.info("Pipeline executado e concluído com SUCESSO.")
            
        except Exception as e:
            logger.critical(f"PIPELINE ABORTADO DEVIDO A FALHA CRÍTICA: {e}")

# ==============================================================================
# Execução Principal (Entry Point)
# ==============================================================================
if __name__ == "__main__":
    pipeline = OrquestradorETL()
    pipeline.executar()
