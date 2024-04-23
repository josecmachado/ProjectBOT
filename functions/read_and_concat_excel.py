import os
import pandas as pd

def read_and_concat_excel(folder_path):
    
    df_source = pd.DataFrame()

    # Itera sobre cada arquivo Excel na pasta
    for arquivo_excel in os.listdir(folder_path):
        if arquivo_excel.endswith('.xlsx'):
            caminho_arquivo = os.path.join(folder_path, arquivo_excel)
            df = pd.read_excel(caminho_arquivo)
            
            # Concatena o DataFrame atual com o DataFrame acumulado
            df_source = pd.concat([df_source, df], ignore_index=True)

    # Exporta a união das planilhas para um arquivo Excel
    
    return df_source