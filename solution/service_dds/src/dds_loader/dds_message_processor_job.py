from logging import Logger
from datetime import datetime
import uuid

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
from dds_loader.repository import OrderDdsBuilder



class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):

            msg = self._consumer.consume()
            if not msg:
                break

            payload = msg['payload']            
            if payload['status'] != 'CLOSED':
                break

            message_in = OrderDdsBuilder(payload)

            self._dds_repository.h_user_insert(message_in.h_user())
            self._dds_repository.h_restaurant_insert(message_in.h_restaurant())
            self._dds_repository.h_order_insert(message_in.h_order())
            self._dds_repository.l_order_user_insert(message_in.l_order_user())
            self._dds_repository.s_user_names_insert(message_in.s_user_names())
            self._dds_repository.s_restaurant_names_insert(message_in.s_restaurant_names())
            self._dds_repository.s_order_cost_insert(message_in.s_order_cost())
            self._dds_repository.s_order_status_insert(message_in.s_order_status())

            for value in message_in.h_product():
                self._dds_repository.h_product_insert(value)
            for value in message_in.h_category():
                self._dds_repository.h_category_insert(value)
            for value in message_in.l_order_product():
                self._dds_repository.l_order_product_insert(value)
            for value in message_in.l_product_restaurant():
                self._dds_repository.l_product_restaurant_insert(value)
            for value in message_in.l_product_category_insert():
                self._dds_repository.insert(value)
            for value in message_in.s_product_names():
                self._dds_repository.s_product_names_insert(value)
            
            h_product_pk_list = [key.h_product_pk for key in message_in.h_product()]
            h_category_pk_list = [key.h_category_pk for key in message_in.h_category()]
            product_name_list = [key.s_product_names for key in message_in.s_product_names()]
            category_name_list = [key.h_categories['category_name'] for key in message_in.h_category()]

            user_id = str(message_in.h_user().h_user_pk)
            dst_msg = {
                "user_id": user_id,
                "product_id": h_product_pk_list,
                "product_name": product_name_list,
                "category_id": h_category_pk_list,
                "category_name": category_name_list
            }

            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")