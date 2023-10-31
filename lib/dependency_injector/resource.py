import logging
import sys

from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Resource


class ResourceInjector(DeclarativeContainer):
    logging = Resource(
        logging.basicConfig,
        force=True,
        level=logging.INFO,
        stream=sys.stdout,
        datefmt="%Y-%m-%dT%H:%M:%S",
        format="%(asctime)s %(name)s[%(levelname)s]:%(message)s",
    )
