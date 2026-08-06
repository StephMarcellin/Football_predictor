"""
logging_config.py — Configuration centralisée de loguru
=======================================================
Point d'entrée unique du logging pour tout le pipeline.

Principe : un module ne configure JAMAIS le logging à l'import. Seuls les
points d'entrée (script lancé en direct, ou l'orchestrateur) appellent
setup_logging(). Cela évite que les imports se marchent dessus sur le
singleton loguru — la cause du bug de logs invisibles.
"""

import sys
from pathlib import Path

from loguru import logger

_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"


def setup_logging(name: str, level: str = "DEBUG", console: bool = True) -> None:
    """
    Configure loguru pour un point d'entrée.

    Repart d'une table rase (logger.remove()) puis rebranche :
      - un sink console (stderr) au niveau INFO si console=True ;
      - un sink fichier logs/{name}.log au niveau `level`, avec rotation.

    Args:
        name:    base du fichier de log → logs/{name}.log (ex. "ingest").
        level:   niveau du sink fichier (défaut "DEBUG").
        console: ajoute le sink console stderr (défaut True).
    """
    Path("logs").mkdir(exist_ok=True)
    logger.remove()  # table rase : état loguru propre et déterministe

    if console:
        logger.add(sys.stderr, level="INFO", format=_FORMAT)

    logger.add(
        f"logs/{name}.log",
        level=level,
        encoding="utf-8",
        rotation="5 MB",
        retention=10,
        format=_FORMAT,
    )