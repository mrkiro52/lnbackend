import json
import pika
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.remote.remote_connection import LOGGER as SELENIUM_LOGGER
from urllib3.connectionpool import log as URLLIB_LOGGER
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SELENIUM_LOGGER.setLevel(logging.WARNING)
URLLIB_LOGGER.setLevel(logging.WARNING)

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME") 
SELENIUM_HUB_URL = os.getenv("SELENIUM_HUB_URL")

class URLConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.driver = None
        
    def setup_selenium(self):
        """Tune Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            
            self.driver = webdriver.Remote(
                command_executor=SELENIUM_HUB_URL,
                options=chrome_options
            )
            logger.info("Selenium WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Selenium WebDriver: {e}")
            raise

    def connect_rabbitmq(self):
        """Connection to RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(
                pika.URLParameters(RABBITMQ_URL)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=QUEUE_NAME, durable=True)
            logger.info("Connected to RabbitMQ successfully")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def process_url(self, url):
        """Processing URL and get HTML"""
        try:
            logger.info(f"Processing URL: {url}")
            
            self.driver.get(url)
            
            time.sleep(3)
            
            html_content = self.driver.page_source
            
            logger.info(f"Page title: {self.driver.title}")
            logger.info(f"Page URL: {self.driver.current_url}")
            logger.info(f"HTML content length: {len(html_content)} characters")
            
            preview = html_content[:500] + "..." if len(html_content) > 500 else html_content
            logger.info(f"HTML preview: {preview}")
            
            return html_content
            
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            return None

    def callback(self, ch, method, properties, body):
        """Callback function to process message from queue"""
        try:
            message = json.loads(body)
            url = message.get('url')
            
            if url:
                html_content = self.process_url(url)
                
                if html_content:
                    logger.info(f"Successfully processed URL: {url}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logger.error(f"Failed to process URL: {url}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                logger.error("Invalid message format: missing URL")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
        except json.JSONDecodeError:
            logger.error("Invalid JSON in message")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Unexpected error in callback: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        try:
            self.setup_selenium()
            self.connect_rabbitmq()
            
            self.channel.basic_qos(prefetch_count=1)
            
            self.channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=self.callback
            )
            
            logger.info("URL Consumer started. Waiting for messages...")
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Selenium WebDriver closed")
        except Exception as e:
            logger.error(f"Error closing WebDriver: {e}")
            
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")

if __name__ == "__main__":
    consumer = URLConsumer()
    consumer.start_consuming()