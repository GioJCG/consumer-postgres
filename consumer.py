from confluent_kafka import Consumer, KafkaError
import psycopg2
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Configuración de Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'cvqocn6qn6pkj5g2nf5g.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'GioJCG',
    'sasl.password': 'FEovgbUIbtjaaCsV2tSzbvyRYZbBPh',
    'group.id': 'movies-consumer-group',  # ¡IMPORTANTE!
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Configuración de PostgreSQL
DB_CONFIG = {
    'host': 'ep-proud-cherry-a42qb93z-pooler.us-east-1.aws.neon.tech',
    'database': 'movies',
    'user': 'neondb_owner',
    'password': 'npg_pq06QGMmydeK',
    'port': '5432'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_movie(data):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
        INSERT INTO movies (title)
        VALUES (%s)
        ON CONFLICT (title) DO NOTHING;
        """
        cur.execute(query, (data['title'],))
        conn.commit()
        
        logging.info(f"Película insertada: {data['title']}")
        return True
        
    except Exception as e:
        logging.error(f"Error al insertar película: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def consume_messages():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['movies_pg'])

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Error de Kafka: {msg.error()}")
                    break

            try:
                data = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Mensaje recibido: {data}")
                
                if insert_movie(data):
                    consumer.commit(msg)
                    
            except json.JSONDecodeError as e:
                logging.error(f"Error decodificando JSON: {e}")
            except Exception as e:
                logging.error(f"Error procesando mensaje: {e}")

    except KeyboardInterrupt:
        logging.info("Deteniendo consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    logging.info("Iniciando consumer...")
    consume_messages()