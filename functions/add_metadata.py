from datetime import datetime
import pandas as pd

def add_metadata(df, source, tool):
    """
    Adiciona colunas de metadados ao DataFrame.

    Parâmetros:
    - df: DataFrame ao qual os metadados serão adicionados.
    - source: Fonte dos dados (por exemplo, "sql_server").
    - tool: Ferramenta utilizada para manipular os dados (por exemplo, "python").
    """
    metadata = {
        "last_update": datetime.now(),
        "source": source,
        "tool": tool
    }

    df['last_update'] = metadata['last_update']
    df['source'] = metadata['source']
    df['tool'] = metadata['tool']