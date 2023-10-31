import logging
from typing import NoReturn

import dotenv

from lib.dependency_injector.resource import ResourceInjector


class MainInjector:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            # is_injected를 init에서 초기화하면 객체가 싱글톤이어도 init은 계속 호출되기 때문에 초기화가 매번 발생된다.
            cls.__instance.__is_injected = False
        return cls.__instance

    def __init__(self):
        self.__configure()
        self.injectors = {}

    def inject(self) -> NoReturn:
        if self.__is_injected:
            logging.info("Dependencies are already injected")
            return
        self.__is_injected = True
        logging.info("Inject Dependencies")

        resource_injector = ResourceInjector()
        resource_injector.init_resources()

    def __configure(self):
        """
        환경 변수 등의 사전 설정
        :return:
        """
        dotenv.load_dotenv()
