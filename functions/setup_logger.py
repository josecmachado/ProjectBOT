import logging

def get_logger(name, level="INFO", log_file=None):
    # Configurar o logger do Python
    logging.basicConfig(level=level)
    logger = logging.getLogger(name)

    # Adicionar um manipulador de arquivo se o caminho do arquivo for fornecido
    if log_file:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.info(f"Logger created for the application: {name}")

    return logger