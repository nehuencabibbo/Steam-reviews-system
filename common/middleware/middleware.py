import logging
import threading
import time
import pika
import pika.exceptions

from common.activity_log.activity_log import ActivityLog
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"


class MiddlewareError(Exception):
    def __init__(self, message=None):
        # super().__init__(message)
        self.message = message

    def __str__(self):
        return (
            f"MiddlewareError: {self.message}"
            if self.message
            else "MiddlewareError has occurred"
        )


class Middleware:
    def __init__(
        self,
        broker_ip,
        protocol: Protocol = Protocol(),
        prefetch_count: int = 100,
        batch_size: int = 10,
        is_async: bool = False,
        on_connected_callback=None,
        use_logging: bool = False,
    ):
        self._connection = (
            self.__create_connection(broker_ip)
            if not is_async
            else self.__create_async_connection(broker_ip, on_connected_callback)
        )
        self._channel = self._connection.channel() if not is_async else None
        if not is_async:
            self._channel.basic_qos(prefetch_count=prefetch_count)
        self.__protocol = protocol
        self.__batch_size = batch_size
        self.__batchs_per_queue = {}
        self.is_running = threading.Event()
        self._logger = None

        if use_logging:
            self._logger = ActivityLog()
            self.__batchs_per_queue = self._logger.recover_middleware_state()

            for queue_name, data in self.__batchs_per_queue.items():
                batch, amount = data
                if amount == self.__batch_size:
                    logging.debug(f'[MIDDLEWARE] While recovering, reached batch limit for {queue_name}, sending {batch}')
                    self._channel.basic_publish(
                        exchange="",
                        routing_key=queue_name,
                        body=batch,
                    )
                    self._logger.remove_queue_state(queue_name)
                    
                    self.__batchs_per_queue[queue_name] = [
                        b"",
                        0,
                    ]

            logging.debug("[MIDDLEWARE] RECOVERED STATE:")
            for queue_name, data in self.__batchs_per_queue.items():
                if 'q1' in queue_name:
                    logging.debug(f"{queue_name}: {data}")

    def __create_connection(self, ip):
        # delete the heartbeat parameter if its too low
        return pika.BlockingConnection(pika.ConnectionParameters(host=ip))

    def __create_async_connection(self, ip, on_connected_callback):
        parameters = pika.ConnectionParameters(host=ip)
        connection = pika.SelectConnection(
            parameters, on_open_callback=on_connected_callback
        )
        return connection

    def create_queue(self, name):
        if not name in self.__batchs_per_queue:
            self.__batchs_per_queue[name] = (
                b"",
                0,
            )  
        self._channel.queue_declare(queue=name)

    def create_anonymous_queue(self):
        result = self._channel.queue_declare(queue="")
        return result.method.queue

    def attach_callback(self, queue_name, callback):
        self._channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False
        )

    def turn_fair_dispatch(self):
        # Fairness
        self._channel.basic_qos(prefetch_count=1)

    def publish_batch(self, queue_name="", exchange_name=""):
        try:
            batch, amount_of_messages = self.__batchs_per_queue[queue_name]

            if amount_of_messages == 0:
                return

            self._channel.basic_publish(
                exchange=exchange_name, routing_key=queue_name, body=batch
            )

            self.__batchs_per_queue[queue_name] = (b"", 0)
        except KeyError:
            return

    def publish_message(self, message: list[str], queue_name="", exchange_name=""):
        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=queue_name,
            body=self.__protocol.add_to_batch(current_batch=b"", row=message),
        )

    def publish(self, message: list[str], queue_name="", exchange_name=""):
        queue_batch, amount_of_messages = self.__batchs_per_queue[queue_name]
        logging.debug(
            f"Publishing: {queue_batch} | amount_of_messages: {amount_of_messages}"
        )
        # prev_batch_length = len(queue_batch)
        # bytes1,bytes2,bytes3,
        new_batch = self.__protocol.add_to_batch(queue_batch, message)

        # Just log the last message for simplificty sake
        # if 'q1' in queue_name:
        #     logging.debug('PREPARE FOR KILL')
        #     time.sleep(0.1)
        if self._logger:
            added_msg = new_batch[len(queue_batch):]
            logging.debug(f"[MIDDLEWARE] Recived message: {added_msg}, for: {queue_name}")
            self._logger.log_for_middleware(queue_name, added_msg)

            logging.debug("[MIDDLEWARE] STATE BEFORE KILL: ")
            for name, data in self.__batchs_per_queue.items():
                if 'q1' in name:
                    logging.debug(f"{name}: {data}")
            
            # if 'q1' in queue_name:
            #     logging.debug("KILL ME")
            #     time.sleep(0.1)

        if amount_of_messages + 1 == self.__batch_size:
            logging.debug(f"[MIDDLEWARE] Sent batch {new_batch} for {queue_name}")
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=queue_name,
                body=new_batch,
            )
            if self._logger: self._logger.remove_queue_state(queue_name)
            logging.debug(f'RESETING BATCH TO 0 FOR {queue_name}')
            self.__batchs_per_queue[queue_name] = (
                b"",
                0,
            )
        else:
            logging.debug(
                f"[MIDDLEWARE] Saved {new_batch[len(queue_batch):]} for {queue_name}"
            )
            self.__batchs_per_queue[queue_name] = (new_batch, amount_of_messages + 1)

    def get_rows_from_message(self, message) -> list[list[str]]:
        return self.__protocol.decode_batch(message)

    def send_end(
        self,
        queue,
        exchange_name="",
        end_message: list[str] = [END_TRANSMISSION_MESSAGE],
    ):
        end_message = self.__protocol.add_to_batch(current_batch=b"", row=end_message)
        self._channel.basic_publish(
            exchange=exchange_name, routing_key=queue, body=end_message
        )

    def start_consuming(self):
        try:
            print(f"Running? {self.is_running}")
            while not self.is_running.is_set():
                self._connection.process_data_events(time_limit=1)
                print("Not blocked at process data events")

        except pika.exceptions.ChannelClosedByBroker as e:
            # Rabbit mq terminated during execution most probably
            # TODO: Is writing to a closed channel handled by this too or
            # does pika.exceptions.ClosedChannel need to be accounted for?
            raise MiddlewareError(message=f"Channel was closed by borker: {e}")
        except pika.exceptions.ConnectionClosed as e:
            # Connection was finished either due to shutdown
            # or general network error
            raise MiddlewareError(f"A connection error ocurred with the broker: {e}")

        except pika.exceptions.StreamLostError as e:
            raise MiddlewareError(f"A connection error ocurred with the broker: {e}")

        except OSError as e:
            raise MiddlewareError("Attempted to send data to a closed socket")

    def process_events_once(self):
        self._connection.process_data_events(time_limit=0)

    def start_async_ioloop(self):
        self._connection.ioloop.start()

    def stop_consuming(self):
        self._channel.stop_consuming()

    def stop_consuming_gracefully(self):
        self._connection.add_callback_threadsafe(self.stop_consuming)

    def ack(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def shutdown(self):
        try:
            self.is_running.set()
            if self._connection.is_open:
                self._connection.add_callback_threadsafe(self._connection.close)

        except pika.exceptions.StreamLostError as e:
            logging.debug(f"CONNECTION ERROR: {e}")

    # Callback should be a function that recives:
    # - delivery_tag: so that it can ack the corresponding message
    # - body: the content of the message itself as bytes
    # - *args: any extra arguments necesary
    @classmethod
    def generate_callback(cls, callback, *args):
        return lambda ch, method, props, body: callback(
            method.delivery_tag, body, *args
        )

    def bind_queue_to_exchange(
        self, exchange_name: str, queue_name: str, exchange_type="fanout"
    ):
        self._channel.exchange_declare(exchange_name, exchange_type)
        self._channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def add_client_id_and_send_batch(
        self, client_id: str, batch: bytes, queue_name: str = "", exchange_name=""
    ):
        self._channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=self.__protocol.insert_before_batch(batch, [client_id]),
        ),

    def execute_from_another_thread(self, fn):
        logging.info(
            f"Executing from another thread: {threading.currentThread().ident}"
        )
        self._connection.add_callback_threadsafe(fn)
        logging.info("added threadsafe callback")

    def check_connection(self):
        try:
            return self._connection.is_open and self._channel.is_open
        except Exception as _:
            logging.debug("The connection with rabbit was closed abruptly")
            return False

